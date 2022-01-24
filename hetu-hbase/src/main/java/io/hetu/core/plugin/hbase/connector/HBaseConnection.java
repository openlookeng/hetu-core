/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hetu.core.plugin.hbase.connector;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.conf.HBaseTableProperties;
import io.hetu.core.plugin.hbase.metadata.HBaseMetastore;
import io.hetu.core.plugin.hbase.metadata.HBaseMetastoreFactory;
import io.hetu.core.plugin.hbase.metadata.HBaseTable;
import io.hetu.core.plugin.hbase.security.HBaseKerberosAuthentication;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.hetu.core.plugin.hbase.utils.StartAndEndKey;
import io.hetu.core.plugin.hbase.utils.Utils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * HBaseConnection
 *
 * @since 2020-03-30
 */
public class HBaseConnection
{
    private static final Logger LOG = Logger.get(HBaseConnection.class);
    // if zookeeper.sasl.client.config not set，then ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME is Client_new，
    // otherwise the kerberos on zookeeper will fail
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client_new";
    private static final String KRB5_CONF_KEY = "java.security.krb5.conf";

    /**
     * hbase config
     */
    protected HBaseConfig hbaseConfig;
    /**
     * hbase Connection
     */
    protected volatile Connection conn;
    /**
     * hbase admin
     */
    protected HBaseAdmin hbaseAdmin;
    /**
     * Configuration
     */
    protected Configuration cfg;
    /**
     * FileSystem
     */
    protected FileSystem fs;

    private UserGroupInformation ugi;
    private HBaseMetastore hbaseMetastore;

    /**
     * constructor
     *
     * @param config HBaseConfig
     */
    @Inject
    public HBaseConnection(HBaseConfig config)
    {
        this.hbaseConfig = config;
        this.hbaseMetastore = new HBaseMetastoreFactory(config).create();
        hbaseMetastore.init();
        authenticate();
        this.conn = createConnection();
    }

    /**
     * constructor for test
     *
     * @param hbaseMetastore HBaseMetaStore
     * @param config HBaseConfig
     */
    protected HBaseConnection(HBaseMetastore hbaseMetastore, HBaseConfig config)
    {
        this.hbaseConfig = config;
        this.hbaseMetastore = hbaseMetastore;
        authenticate();
    }

    /**
     * getDefaultValue
     *
     * @return String
     */
    public String getDefaultValue()
    {
        return this.hbaseConfig.getDefaultValue();
    }

    /**
     * getHbaseConfig
     *
     * @return HBaseConfig
     */
    public HBaseConfig getHbaseConfig()
    {
        return hbaseConfig;
    }

    private void authenticate()
    {
        cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", hbaseConfig.getZkQuorum());
        cfg.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZkClientPort());
        cfg.set("zookeeper.znode.parent", hbaseConfig.getZkZnodeParent());
        cfg.set("hbase.client.retries.number", hbaseConfig.getRetryNumber() + "");
        cfg.set("hbase.client.pause", hbaseConfig.getPauseTime() + "");

        try {
            if (hbaseConfig.isClientSideEnable()) {
                cfg.set("hbase.cluster.distributed", "true");
                cfg.set("hbase.mob.file.cache.size", "0");
                if (!Utils.isFileExist(hbaseConfig.getCoreSitePath())) {
                    throw new FileNotFoundException(hbaseConfig.getCoreSitePath());
                }
                cfg.addResource(new Path(hbaseConfig.getCoreSitePath()));
                if (!Utils.isFileExist(hbaseConfig.getHdfsSitePath())) {
                    throw new FileNotFoundException(hbaseConfig.getHdfsSitePath());
                }
                cfg.addResource(new Path(hbaseConfig.getHdfsSitePath()));
                cfg.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            }

            if (Constants.HDFS_AUTHENTICATION_KERBEROS.equals(hbaseConfig.getKerberos())) {
                if (!Utils.isFileExist(hbaseConfig.getHbaseSitePath())) {
                    throw new FileNotFoundException(hbaseConfig.getHbaseSitePath());
                }
                cfg.addResource(new Path(hbaseConfig.getHbaseSitePath()));
                if (hbaseConfig.getJaasConfPath() != null && !hbaseConfig.getJaasConfPath().isEmpty()) {
                    System.setProperty("java.security.auth.login.config", hbaseConfig.getJaasConfPath());
                }
                if (hbaseConfig.getKrb5ConfPath() != null && !hbaseConfig.getKrb5ConfPath().isEmpty()) {
                    System.setProperty("java.security.krb5.conf", hbaseConfig.getKrb5ConfPath());
                }
                if (hbaseConfig.isRpcProtectionEnable()) {
                    cfg.set("hbase.rpc.protection", "privacy");
                }

                cfg.set("username.client.keytab.file", hbaseConfig.getUserKeytabPath());
                cfg.set("username.client.kerberos.principal", hbaseConfig.getPrincipalUsername());
                cfg.set("hadoop.security.authentication", "Kerberos");
                cfg.set("hbase.security.authentication", "Kerberos");

                String userName = hbaseConfig.getPrincipalUsername();
                String userKeytabFile = hbaseConfig.getUserKeytabPath();
                String krb5File = System.getProperty("java.security.krb5.conf");
                HBaseKerberosAuthentication.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
                ugi = HBaseKerberosAuthentication.authenticateAndReturnUGI(userName, userKeytabFile, krb5File, cfg);
            }
        }
        catch (IOException e) {
            LOG.error("auth failed...cause by %s", e);
        }
    }

    /**
     * getConfiguration
     *
     * @return Configuration
     */
    public Configuration getConfiguration()
    {
        return cfg;
    }

    /**
     * getFileSystem
     *
     * @return FileSystem
     */
    public FileSystem getFileSystem() throws IOException
    {
        return (fs == null) ? FileSystem.get(cfg) : fs;
    }

    /**
     * getConnection
     *
     * @return Connection
     */
    public Connection getConn()
    {
        return (conn == null) ? createConnection() : conn;
    }

    /**
     * getConn
     *
     * @return Connection
     */
    public Connection createConnection()
    {
        final Connection[] connection = new Connection[1];
        if (Constants.HDFS_AUTHENTICATION_KERBEROS.equals(hbaseConfig.getKerberos())) {
            ugi.doAs(
                    new PrivilegedAction<Connection>()
                    {
                        /**
                         * @return Object
                         */
                        public Connection run()
                        {
                            try {
                                connection[0] = ConnectionFactory.createConnection(cfg);
                            }
                            catch (IOException e) {
                                LOG.error("[kerberos] create hbase connector failed...cause by : %s", e);
                            }
                            return connection[0];
                        }
                    });
        }
        else {
            try {
                connection[0] = ConnectionFactory.createConnection(cfg);
            }
            catch (IOException e) {
                LOG.error("create hbase connection failed...cause by: %s", e);
                connection[0] = null;
            }
        }

        return connection[0];
    }

    /**
     * getHbaseAdmin
     *
     * @return HBaseAdmin
     */
    public HBaseAdmin getHbaseAdmin()
    {
        try {
            Admin admin = this.getConn().getAdmin();
            if (admin instanceof HBaseAdmin) {
                hbaseAdmin = (HBaseAdmin) admin;
            }
        }
        catch (IOException e) {
            LOG.error("getHbaseAdmin failed... cause by %s", e);
            hbaseAdmin = null;
            throw new PrestoException(
                    HBaseErrorCode.UNEXPECTED_HBASE_ERROR,
                    format("getHbaseAdmin failed... cause by %s", e.getMessage()));
        }
        return hbaseAdmin;
    }

    /**
     * listSchemaNames
     *
     * @return List
     */
    public List<String> listSchemaNames()
    {
        List<String> schemas = new ArrayList<>();
        try {
            NamespaceDescriptor[] namespaces = this.getHbaseAdmin().listNamespaceDescriptors();
            for (NamespaceDescriptor namespace : namespaces) {
                schemas.add(namespace.getName());
            }
        }
        catch (IOException e) {
            LOG.error("listSchemaNames failed... cause by %s", e);
        }
        return schemas;
    }

    /**
     * createSchema
     *
     * @param schemaName schemaName
     */
    public void createSchema(String schemaName)
    {
        try {
            NamespaceDescriptor[] namespaces = this.getHbaseAdmin().listNamespaceDescriptors();
            for (NamespaceDescriptor namespace : namespaces) {
                if (namespace.getName().equals(schemaName)) {
                    throw new PrestoException(
                            INVALID_TABLE_PROPERTY, format("Schema already exist in hbase: %s", schemaName));
                }
            }
            NamespaceDescriptor newNamespace = NamespaceDescriptor.create(schemaName).build();
            this.getHbaseAdmin().createNamespace(newNamespace);
        }
        catch (IOException e) {
            LOG.error("create schema failed... cause by %s", e);
            throw new PrestoException(
                    INVALID_TABLE_PROPERTY,
                    format("create schema[%s] failed caused by %s", schemaName, e.getMessage()));
        }
    }

    /**
     * dropSchema
     *
     * @param schemaName schemaName
     */
    public void dropSchema(String schemaName)
    {
        try {
            // at first，check there is nothing in the schema named schemaName
            // if there is, stop to drop schema, if there isn't, drop schema
            if (this.getHbaseAdmin().listTableDescriptorsByNamespace(schemaName).length > 0) {
                LOG.warn("dropSchema: there are some tables under the schema[%s], can't drop the schema.", schemaName);
                throw new IOException(
                        "there are some tables under the schema, "
                                + "can't drop the schema before delete all table under the schema");
            }
            getHbaseAdmin().deleteNamespace(schemaName);
        }
        catch (IOException e) {
            LOG.error("drop schema failed... cause by %s", e);
            throw new PrestoException(
                    INVALID_TABLE_PROPERTY, format("drop schema[%s] failed cause by %s. ", schemaName, e.getMessage()));
        }
    }

    /**
     * addColumn
     * <p>
     * ALTER TABLE name ADD COLUMN column_name data_type WITH ( family = 'f', qualifier = 'q')
     *
     * @param tableHandle tableHandle
     * @param column column
     */
    public void addColumn(HBaseTableHandle tableHandle, ColumnMetadata column)
    {
        HBaseTable htable = hbaseMetastore.getHBaseTable(tableHandle.getFullTableName());
        if (htable == null) {
            throw new PrestoException(
                    HBaseErrorCode.HBASE_TABLE_DNE,
                    format("addColumn fail, cause table[%s] not exists.", tableHandle.getTable()));
        }

        int ordinal = htable.getColumns().size();

        // check newColumn whether exists
        if (htable.getColumnsMap().get(column.getName()) != null) {
            throw new PrestoException(
                    HBaseErrorCode.HBASE_TABLE_DNE,
                    format(
                            "addColumn fail, cause the column[%s] already exists in table[%s] .",
                            column.getName(), tableHandle.getTable()));
        }

        HBaseColumnHandle newColumn = getColumn(column, ordinal);

        // check family is exists
        List<String> familiers = queryTableFamilys(htable.getHbaseTableName().get());
        if (familiers.size() == 0) {
            // table not exits...
            throw new PrestoException(
                    HBaseErrorCode.HBASE_TABLE_DNE,
                    format("Table %s does not exist any more", htable.getHbaseTableName().get()));
        }
        else {
            // family not exists, then add family
            if (!familiers.contains(newColumn.getFamily().get())) {
                addNewFamily(htable.getHbaseTableName().get(), newColumn.getFamily().get());
            }
            else {
                // family exist and qualifier exist
                if (hasSamePair(htable.getColumns(), newColumn)) {
                    throw new PrestoException(
                            INVALID_TABLE_PROPERTY,
                            "Duplicate column family/qualifier pair d"
                                    + "etected in column mapping, check the value of "
                                    + HBaseTableProperties.COLUMN_MAPPING);
                }
            }
        }

        htable.getColumns().add(newColumn);
        htable.getColumnsMap().put(newColumn.getName(), newColumn);
        htable.getColumnMetadatas().add(newColumn.getColumnMetadata());

        hbaseMetastore.dropHBaseTable(htable);
        hbaseMetastore.addHBaseTable(htable);
    }

    /**
     * addNewFamily
     *
     * @param table table
     * @param family family
     */
    protected void addNewFamily(String table, String family)
    {
        try {
            this.getHbaseAdmin().addColumnFamily(TableName.valueOf(table), ColumnFamilyDescriptorBuilder.of(family));
        }
        catch (IOException e) {
            LOG.error("addNewFamily failed, cause by %s", e);
            throw new PrestoException(INVALID_TABLE_PROPERTY, "add new family failed, cause by " + e.getMessage());
        }
    }

    /**
     * check whether new column has same family/qualifier pair with table
     *
     * @param columns hbase columns
     * @param newColumn new column
     * @return boolean
     */
    protected boolean hasSamePair(List<HBaseColumnHandle> columns, HBaseColumnHandle newColumn)
    {
        for (HBaseColumnHandle column : columns) {
            if (column.getFamily().isPresent()
                    && column.getFamily().get().equals(newColumn.getFamily().get())
                    && column.getQualifier().get().equals(newColumn.getQualifier().get())) {
                return true;
            }
        }
        return false;
    }

    /**
     * getTable
     *
     * @param table schematable
     * @return hbaseTable
     */
    public HBaseTable getTable(SchemaTableName table)
    {
        requireNonNull(table, "schema table name is null");

        String tableName = table.getSchemaName() + Constants.POINT + table.getTableName();

        return getTable(tableName);
    }

    /**
     * getTable
     *
     * @param table table
     * @return HBaseTable
     */
    public HBaseTable getTable(String table)
    {
        String tableName = table;

        // check whether table exist or not in Memory or hbasetable'store
        return hbaseMetastore.getHBaseTable(tableName);
    }

    /**
     * get all tables from metadata set
     *
     * @return all tables metadata
     */
    public Map<String, List<SchemaTableName>> getAllTablesFromMetadata()
    {
        Map<String, List<SchemaTableName>> map = new HashMap<>();
        List<SchemaTableName> schemaNames;
        for (String table : hbaseMetastore.getAllHBaseTables().keySet()) {
            SchemaTableName st =
                    new SchemaTableName(table.split(Constants.SPLITPOINT)[0], table.split(Constants.SPLITPOINT)[1]);
            schemaNames = map.get(table.split(Constants.SPLITPOINT)[0]);
            if (schemaNames == null) {
                schemaNames = new ArrayList<>();
            }
            schemaNames.add(st);
            map.put(table.split(Constants.SPLITPOINT)[0], schemaNames);
        }

        return map;
    }

    /**
     * createTable
     *
     * @param meta connector metadata
     * @return hbasetable
     */
    public HBaseTable createTable(ConnectorTableMetadata meta)
    {
        // Validate the DDL is something we can handle
        validateCreateTable(meta);

        Map<String, Object> tableProperties = meta.getProperties();
        String rowIdColumn = getRowIdColumn(meta);
        // Get the list of column handles
        List<HBaseColumnHandle> columns = getColumnHandles(meta, rowIdColumn);
        Map<String, ColumnHandle> columnHandleMap = new ConcurrentHashMap();

        for (HBaseColumnHandle column : columns) {
            columnHandleMap.put(column.getName(), column);
        }

        HBaseTable table =
                new HBaseTable(
                        meta.getTable().getSchemaName(),
                        meta.getTable().getTableName(),
                        columns,
                        rowIdColumn,
                        HBaseTableProperties.isExternal(tableProperties),
                        HBaseTableProperties.getSerializerClass(tableProperties),
                        HBaseTableProperties.getIndexColumnsAsStr(meta.getProperties()),
                        HBaseTableProperties.getHBaseTableName(meta.getProperties()),
                        HBaseTableProperties.getSplitByChar(meta.getProperties()));

        table.setColumnsToMap(columnHandleMap);

        try {
            // check whether hbase table exist
            if (table.getHbaseTableName().isPresent()) {
                String hbaseTableName = table.getHbaseTableName().get();
                if (existTable(hbaseTableName)) {
                    // hbase has exist this table, so update the tableCatalog for new
                    // check the family exist or not in hbase table
                    checkFamilyExist(table, rowIdColumn);
                    hbaseMetastore.addHBaseTable(table);
                    return table;
                }
                else {
                    throw new PrestoException(
                            HBaseErrorCode.HBASE_TABLE_DNE,
                            format("Table %s does not exist", table.getHbaseTableName().get()));
                }
            }
            else {
                String hbaseTableName = table.getFullTableName().replace(Constants.POINT, Constants.SEPARATOR);
                table.setHbaseTableName(Optional.of(hbaseTableName));
                if (table.isExternal() && !existTable(hbaseTableName)) {
                    throw new PrestoException(
                            HBaseErrorCode.HBASE_CREATE_ERROR,
                            format("Use lk creating new HBase table [%s], we must specify 'with(external=false)'. ", table.getTable()));
                }

                // create namespace if not exist
                createNamespaceIfNotExist(this.getHbaseAdmin(), table.getSchema());
                // create hbase table
                createHBaseTable(table);
                // save tableCatalog to memory and file
                hbaseMetastore.addHBaseTable(table);
                return table;
            }
        }
        catch (IOException e) {
            LOG.error("createTable: create table failed... cause by %s", e);
            throw new PrestoException(
                    HBaseErrorCode.HBASE_TABLE_DNE, "createTable: create table failed... cause by " + e);
        }
    }

    /**
     * when create external table, check family exist
     *
     * @param table HBaseTable
     * @param rowIdColumn rowIdColumn
     */
    protected void checkFamilyExist(HBaseTable table, String rowIdColumn)
    {
        String hbaseTableName = table.getHbaseTableName().get();
        List<String> familiers = queryTableFamilys(hbaseTableName);
        for (HBaseColumnHandle hch : table.getColumns()) {
            if (!hch.getName().equals(rowIdColumn) && !familiers.contains(hch.getFamily().get())) {
                throw new PrestoException(
                        INVALID_TABLE_PROPERTY,
                        "the exist hbase table "
                                + hbaseTableName
                                + " not constain family["
                                + hch.getFamily().get()
                                + "]");
            }
        }
    }

    /**
     * create HBase table
     *
     * @param table table
     * @throws IOException IOException
     */
    protected void createHBaseTable(HBaseTable table)
            throws IOException
    {
        TableName tableName = TableName.valueOf(Bytes.toBytes(table.getHbaseTableName().get()));
        // builder table
        HTableDescriptor htd = new HTableDescriptor(tableName);

        // add column family to table
        String family;
        for (HBaseColumnHandle cm : table.getColumns()) {
            if (cm.getFamily().orElse(null) != null) {
                // add column family
                family = cm.getFamily().get();
                // check whether the family exists
                if (!htd.hasFamily(family.getBytes(UTF_8))) {
                    HColumnDescriptor hcd = new HColumnDescriptor(family);
                    htd.addFamily(hcd);
                }
            }
        }

        List<StartAndEndKey> allRanges = Arrays.stream(table.getSplitByChar().get().split(","))
                .map(StartAndEndKey::new).collect(Collectors.toList());
        int rangeLength = 0;
        for (StartAndEndKey allRange : allRanges) {
            rangeLength += (Math.abs(allRange.getEnd() - allRange.getStart()) + 1);
        }
        if (rangeLength > Constants.START_END_KEYS_COUNT) {
            this.getHbaseAdmin().createTable(htd);
            return;
        }

        List<byte[]> splitKeys = new ArrayList<>();
        allRanges.forEach(range -> {
            for (char index = range.getStart(); index <= range.getEnd(); index += 1) {
                splitKeys.add(String.valueOf(index).getBytes(UTF_8));
            }
        });

        // create table
        this.getHbaseAdmin().createTable(htd, splitKeys.toArray(new byte[][] {new byte[] {0}}));
    }

    /**
     * when create table, if table exist in hbase, we should assure that family exist.
     *
     * @param tableName tableName
     * @return List
     */
    protected List<String> queryTableFamilys(String tableName)
    {
        HTableDescriptor table;
        List<String> familiers = new ArrayList<>();
        try {
            table = this.getHbaseAdmin().getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor[] cfds = table.getColumnFamilies();
            for (HColumnDescriptor cfd : cfds) {
                familiers.add(cfd.getNameAsString());
            }
        }
        catch (IOException e) {
            LOG.error("queryTableFamilys failed... cause by %s", e.getMessage());
        }

        return familiers;
    }

    /**
     * dropTable
     *
     * @param table table
     */
    public void dropTable(HBaseTable table)
    {
        // check whether table exist in memory or not
        if (hbaseMetastore.getHBaseTable(table.getFullTableName()) != null) {
            try {
                // if table is not external, drop hbase table
                if (!table.isExternal()) {
                    deleteTableIgnoreExistOrNot(table.getHbaseTableName().get());
                }

                hbaseMetastore.dropHBaseTable(table);
            }
            catch (IOException e) {
                LOG.error("deleteTableIgnoreExistOrNot: when delete table, cause by %s", e);
                throw new PrestoException(INVALID_TABLE_PROPERTY, "drop table[" + table.getTable() + "] failed.");
            }
        }
        else {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "table[" + table.getTable() + "] not exists.");
        }
    }

    /**
     * renameTable
     *
     * @param oldName oldName
     * @param newName newName
     */
    public void renameTable(SchemaTableName oldName, SchemaTableName newName)
    {
        if (!oldName.getSchemaName().equals(newName.getSchemaName())) {
            throw new PrestoException(
                    NOT_SUPPORTED, "HBase does not support renaming tables to different namespaces (schemas)");
        }

        HBaseTable oldTable = getTable(oldName);
        if (oldTable == null) {
            throw new TableNotFoundException(oldName);
        }

        // validate table exist in hetu
        String oldTableName = oldName.getSchemaName() + Constants.POINT + oldName.getTableName();
        String newTableName = newName.getSchemaName() + Constants.POINT + newName.getTableName();
        if (getTable(newName) != null) {
            // table exist
            throw new PrestoException(
                    HBaseErrorCode.HBASE_TABLE_EXISTS, format("Table %s already exists", newTableName));
        }

        oldTable.setTable(newName.getTableName());
        oldTable.setSchema(newName.getSchemaName());
        hbaseMetastore.renameHBaseTable(oldTable, oldTableName);
    }

    /**
     * getSchemaNames
     *
     * @return set
     */
    public Set<String> getSchemaNames()
    {
        Set<String> schemas = new HashSet<>();
        try {
            NamespaceDescriptor[] namespaces = this.getHbaseAdmin().listNamespaceDescriptors();
            for (NamespaceDescriptor namespace : namespaces) {
                schemas.add(namespace.getName());
            }
        }
        catch (IOException e) {
            LOG.error("listSchemaNames failed... cause by %s", e);
            throw new PrestoException(
                    HBaseErrorCode.UNEXPECTED_HBASE_ERROR, format("listSchemaNames failed. cause: %s", e.getMessage()));
        }

        return schemas;
    }

    /**
     * existTable
     *
     * @param tableName tableName
     * @return default value = false
     */
    protected boolean existTable(String tableName)
    {
        boolean flag = false;
        try {
            flag = this.getHbaseAdmin().getTableDescriptor(TableName.valueOf(tableName)) != null;
        }
        catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            LOG.info("existTable: table[%s] is not exists...", tableName);
        }
        catch (IOException e) {
            LOG.error("invoke existTable failed...cause by %s", e);
        }
        return flag;
    }

    /**
     * deleteTableIgnoreExistOrNot
     *
     * @param tableName tableName
     * @throws IOException tableNotFoundException
     */
    protected void deleteTableIgnoreExistOrNot(String tableName)
            throws IOException
    {
        try {
            this.getHbaseAdmin().disableTable(TableName.valueOf(tableName));
            this.getHbaseAdmin().deleteTable(TableName.valueOf(tableName));
        }
        catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            LOG.warn("deleteTableIgnoreExistOrNot: table[%s] not exist... ", tableName);
        }
    }

    /**
     * Validates the given metadata for a series of conditions to ensure the table is well-formed.
     *
     * @param meta Table metadata
     */
    private void validateCreateTable(ConnectorTableMetadata meta)
    {
        validateColumns(meta);
    }

    /**
     * Check whether the namespace is exist. If not, create it.
     *
     * @param hbaseAdmin hbaseAdmin
     * @param namespace namespace
     */
    protected static void createNamespaceIfNotExist(HBaseAdmin hbaseAdmin, String namespace)
    {
        try {
            boolean flag = false;
            for (NamespaceDescriptor nn : hbaseAdmin.listNamespaceDescriptors()) {
                if (namespace.equals(nn.getName())) {
                    flag = true;
                }
            }

            if (!flag) {
                hbaseAdmin.createNamespace(NamespaceDescriptor.create(namespace).build());
            }
        }
        catch (IOException e) {
            LOG.error("createNamespaceIfNotExist failed... cause by %s", e);
        }
    }

    private static HBaseColumnHandle getColumn(ColumnMetadata column, int ordinal)
    {
        Map<String, Object> properties = column.getProperties();
        Object familyObj = properties.get("family");
        Object qualifierObj = properties.get("qualifier");
        if (familyObj == null || qualifierObj == null) {
            throw new PrestoException(
                    INVALID_TABLE_PROPERTY,
                    format("column[%s]'s family and qualifier should not be null or empty ", column.getName()));
        }

        if (familyObj instanceof String && qualifierObj instanceof String) {
            String family = (String) familyObj;
            String qualifier = (String) qualifierObj;

            return new HBaseColumnHandle(
                    column.getName(),
                    Optional.of(family),
                    Optional.of(qualifier),
                    column.getType(),
                    ordinal,
                    format("HBase column %s:%s. Indexed: false", family, qualifier),
                    false);
        }

        throw new PrestoException(
                INVALID_TABLE_PROPERTY,
                format("the type of column[%s]'s family and qualifier should be string", column.getName()));
    }

    private static List<HBaseColumnHandle> getColumnHandles(ConnectorTableMetadata meta, String rowIdColumn)
    {
        // Get the column mappings from the table property or auto-generate columns if not defined
        Map<String, Pair<String, String>> mapping =
                HBaseTableProperties.getColumnMapping(meta.getProperties())
                        .orElse(
                                autoGenerateMapping(
                                        meta.getColumns(),
                                        HBaseTableProperties.getLocalityGroups(meta.getProperties())));

        // The list of indexed columns
        Optional<List<String>> indexedColumns = HBaseTableProperties.getIndexColumns(meta.getProperties());

        // And now we parse the configured columns and create handles for the metadata manager
        List<HBaseColumnHandle> columns = new ArrayList<>();
        for (int ordinal = 0; ordinal < meta.getColumns().size(); ++ordinal) {
            ColumnMetadata cm = meta.getColumns().get(ordinal);

            // Special case if this column is the row ID
            if (cm.getName().equalsIgnoreCase(rowIdColumn)) {
                columns.add(
                        new HBaseColumnHandle(
                                rowIdColumn,
                                Optional.empty(),
                                Optional.empty(),
                                cm.getType(),
                                ordinal,
                                "HBase row ID",
                                false));
            }
            else {
                if (!mapping.containsKey(cm.getName())) {
                    LOG.error("Misconfigured mapping for HBase column %s", cm.getName());
                    throw new InvalidParameterException(
                            format("Misconfigured mapping for HBase column %s", cm.getName()));
                }

                // Get the mapping for this column
                Pair<String, String> familyQualifier = mapping.get(cm.getName());
                boolean indexed =
                        indexedColumns.isPresent()
                                && indexedColumns.get().contains(cm.getName().toLowerCase(Locale.ENGLISH));
                String comment =
                        format(
                                "HBase column %s:%s. Indexed: %b",
                                familyQualifier.getLeft(), familyQualifier.getRight(), indexed);

                // Create a new HBaseColumnHandle object
                columns.add(
                        new HBaseColumnHandle(
                                cm.getName(),
                                Optional.of(familyQualifier.getLeft()),
                                Optional.of(familyQualifier.getRight()),
                                cm.getType(),
                                ordinal,
                                comment,
                                indexed));
            }
        }

        return columns;
    }

    private static Map<String, Pair<String, String>> autoGenerateMapping(
            List<ColumnMetadata> columns, Optional<Map<String, Set<String>>> groups)
    {
        Map<String, Pair<String, String>> mapping = new HashMap<>();
        // all columns in a single family
        final String familyConstant = "family";
        for (ColumnMetadata column : columns) {
            Optional<String> family = getColumnLocalityGroup(column.getName(), groups);
            mapping.put(column.getName(), Pair.of(family.orElse(familyConstant), column.getName()));
        }
        return mapping;
    }

    /**
     * find the group that a column name belongs to if the localityGroup not empty
     *
     * @param columnName column name
     * @param groups locality groups
     * @return return the group name that column belongs
     */
    private static Optional<String> getColumnLocalityGroup(
            String columnName, Optional<Map<String, Set<String>>> groups)
    {
        if (groups.isPresent()) {
            for (String family : groups.get().keySet()) {
                if (groups.get().get(family).contains(columnName.toLowerCase(Locale.ENGLISH))) {
                    return Optional.of(family);
                }
            }
        }

        return Optional.empty();
    }

    private static void validateColumns(ConnectorTableMetadata meta)
    {
        // Check schema name
        if (!validateSchemaName(meta.getTable().getSchemaName(), meta.getProperties())) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "SchemaName needs to consistent with HBase schema");
        }
        // Check columns type
        Set<String> columns = new HashSet<>();
        for (ColumnMetadata column : meta.getColumns()) {
            checkTypeValidate(column.getType());
            // check not exists duplicate columns
            if (!columns.add(column.getName().toLowerCase(Locale.ENGLISH))) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "not support duplicate column names");
            }
        }

        Optional<Map<String, Pair<String, String>>> columnMapping = HBaseTableProperties.getColumnMapping(meta.getProperties());
        if (columnMapping.isPresent()) {
            // check no duplicates in the column mapping
            Set<String> mappings = new HashSet<>();
            for (Map.Entry<String, Pair<String, String>> entry : columnMapping.get().entrySet()) {
                if (!mappings.add(entry.getValue().getLeft() + "/" + entry.getValue().getRight())) {
                    throw new PrestoException(
                            INVALID_TABLE_PROPERTY,
                            "Duplicate column family/qualifier pair detected in column mapping, check the value of column_mapping");
                }
            }
        }
        else {
            if (HBaseTableProperties.getHBaseTableName(meta.getProperties()).isPresent()) {
                // map to a HBase table without column_mapping
                throw new PrestoException(
                        INVALID_TABLE_PROPERTY,
                        "Column generation for mapping hbase tables is not supported, must specify column_mapping");
            }
        }
    }

    /**
     * Gets the row ID based on a table properties or the first column name.
     *
     * @param meta ConnectorTableMetadata
     * @return Lowercase Hetu column name mapped to the HBase row ID
     */
    private static String getRowIdColumn(ConnectorTableMetadata meta)
    {
        Optional<String> rowIdColumn = HBaseTableProperties.getRowId(meta.getProperties());
        return rowIdColumn.orElse(meta.getColumns().get(0).getName()).toLowerCase(Locale.ENGLISH);
    }

    private static boolean checkTypeValidate(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return true;
        }
        else if (type.equals(DOUBLE)) {
            return true;
        }
        else if (type.equals(BIGINT)) {
            return true;
        }
        else if (type.equals(INTEGER)) {
            return true;
        }
        else if (type.equals(SMALLINT)) {
            return true;
        }
        else if (type.equals(TINYINT)) {
            return true;
        }
        else if (type.equals(DATE)) {
            return true;
        }
        else if (type.equals(TIME)) {
            return true;
        }
        else if (type.equals(TIMESTAMP)) {
            return true;
        }
        else if (type instanceof VarcharType) {
            return true;
        }
        else {
            LOG.error("Create table : Unsupported type %s", type);
            throw new PrestoException(NOT_SUPPORTED, "type: " + type + " is not supported");
        }
    }

    private static boolean validateSchemaName(String schemaName, Map<String, Object> tableProperties)
    {
        Optional<String> hbaseTableName = HBaseTableProperties.getHBaseTableName(tableProperties);
        final int sizeOne = 1;
        final int sizeTwo = 2;

        if (hbaseTableName.isPresent()) {
            String[] fullTableName = hbaseTableName.get().split(Constants.SEPARATOR);
            if (fullTableName.length == sizeOne && !schemaName.equals(Constants.DEFAULT)) {
                return false;
            }
            if (fullTableName.length == sizeTwo && !schemaName.equals(fullTableName[0])) {
                return false;
            }
        }
        return true;
    }
}
