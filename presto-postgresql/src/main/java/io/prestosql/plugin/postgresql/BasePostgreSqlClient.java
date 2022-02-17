/*
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
package io.prestosql.plugin.postgresql;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BlockReadFunction;
import io.prestosql.plugin.jdbc.BlockWriteFunction;
import io.prestosql.plugin.jdbc.BooleanWriteFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DoubleWriteFunction;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.WriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.WriteNullFunction;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.postgresql.TypeUtils.getJdbcObjectArray;
import static io.prestosql.plugin.postgresql.TypeUtils.jdbcObjectArrayToBlock;
import static io.prestosql.plugin.postgresql.TypeUtils.toBoxedArray;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Locale.ENGLISH;

public abstract class BasePostgreSqlClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(BasePostgreSqlClient.class);

    /**
     * If disabled, do not accept sub-query push down.
     */
    private final JdbcPushDownModule pushDownModule;
    protected static final String DUPLICATE_TABLE_SQLSTATE = "42P07";

    protected final Type jsonType;
    protected final Type uuidType;
    protected final boolean supportArrays;

    public BasePostgreSqlClient(
            BaseJdbcConfig config,
            PostgreSqlConfig postgresqlConfig,
            @StatsCollecting ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, "\"", connectionFactory);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.pushDownModule = config.getPushDownModule();

        switch (postgresqlConfig.getArrayMapping()) {
            case DISABLED:
                supportArrays = false;
                break;
            case AS_ARRAY:
                supportArrays = true;
                break;
            default:
                throw new IllegalArgumentException("Unsupported ArrayMapping: " + postgresqlConfig.getArrayMapping());
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new PrestoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in PostgreSQL");
        }

        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTable.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    protected ResultSet optimizedGetColumns(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        return getColumns(tableHandle, connection.getMetaData());
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            Map<String, Integer> arrayColumnDimensions = getArrayColumnDimensions(connection, tableHandle);
            try (ResultSet resultSet = optimizedGetColumns(connection, tableHandle)) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.of(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)));
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, Integer> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        if (!supportArrays) {
            return ImmutableMap.of();
        }
        String sql = "" +
                "SELECT att.attname, greatest(att.attndims, 1) AS attndims " +
                "FROM pg_attribute att " +
                "  JOIN pg_type attyp ON att.atttypid = attyp.oid" +
                "  JOIN pg_class tbl ON tbl.oid = att.attrelid " +
                "  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid " +
                "WHERE ns.nspname = ? " +
                "AND tbl.relname = ? " +
                "AND attyp.typcategory = 'A' ";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableHandle.getSchemaName());
            statement.setString(2, tableHandle.getTableName());

            Map<String, Integer> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("attname"), resultSet.getInt("attndims"));
                }
            }
            return arrayColumnDimensions;
        }
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    protected static ColumnMapping timestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                (resultSet, columnIndex) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
                    return packDateTimeWithZone(millisUtc, UTC_KEY);
                },
                timestampWithTimeZoneWriteFunction());
    }

    protected static LongWriteFunction timestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new java.sql.Timestamp(millisUtc));
        };
    }

    protected static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, String elementJdbcTypeName)
    {
        return ColumnMapping.blockMapping(
                arrayType,
                arrayReadFunction(session, arrayType.getElementType()),
                arrayWriteFunction(session, arrayType.getElementType(), elementJdbcTypeName));
    }

    protected static BlockReadFunction arrayReadFunction(ConnectorSession session, Type elementType)
    {
        return (resultSet, columnIndex) -> {
            Object[] objectArray = toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return jdbcObjectArrayToBlock(session, elementType, objectArray);
        };
    }

    protected static BlockWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(elementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        };
    }

    protected JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        try {
            TypeInfo typeInfo = connection.unwrap(PgConnection.class).getTypeInfo();
            int pgElementOid = typeInfo.getPGArrayElement(typeInfo.getPGType(jdbcTypeName));
            return new JdbcTypeHandle(
                    typeInfo.getSQLType(pgElementOid),
                    Optional.of(typeInfo.getPGType(pgElementOid)),
                    arrayTypeHandle.getColumnSize(),
                    arrayTypeHandle.getDecimalDigits(),
                    arrayTypeHandle.getArrayDimensions());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected abstract ColumnMapping jsonColumnMapping();

    protected abstract ColumnMapping typedVarcharColumnMapping(String jdbcTypeName);

    protected static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> {
            UUID uuid = new UUID(value.getLong(0), value.getLong(SIZE_OF_LONG));
            statement.setObject(index, uuid, Types.OTHER);
        };
    }

    protected static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    protected ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    protected static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    protected static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    protected static Slice jsonParse(Slice slice)
    {
        try (JsonParser parser = createJsonParser(slice)) {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // the function nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    protected static JsonParser createJsonParser(Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass an InputStreamReader instead.
        return JSON_FACTORY.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;
        jdbcTableHandle.setUpdatedColumnTypes(updatedColumnTypes);
        jdbcTableHandle.setDeleteOrUpdate(true);
        return jdbcTableHandle;
    }

    private void setStatement(ConnectorSession session, ConnectorTableHandle tableHandle, PreparedStatement statement, Block block, int position, int channel)
            throws SQLException
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;
        List<Type> updatedColumnTypes = jdbcTableHandle.getUpdatedColumnTypes();

        List<WriteMapping> writeMappings = updatedColumnTypes.stream()
                .map(type ->
                {
                    WriteMapping writeMapping = toWriteMapping(session, type);
                    WriteFunction writeFunction = writeMapping.getWriteFunction();
                    verify(
                            type.getJavaType() == writeFunction.getJavaType(),
                            "openLooKeng type %s is not compatible with write function %s accepting %s",
                            type,
                            writeFunction,
                            writeFunction.getJavaType());
                    return writeMapping;
                })
                .collect(toImmutableList());

        List<WriteFunction> columnWriters = writeMappings.stream()
                .map(WriteMapping::getWriteFunction)
                .collect(toImmutableList());

        List<WriteNullFunction> nullWriters = writeMappings.stream()
                .map(WriteMapping::getWriteNullFunction)
                .collect(toImmutableList());

        int parameterIndex = channel + 1;

        if (block.isNull(position)) {
            nullWriters.get(channel).setNull(statement, parameterIndex);
            return;
        }

        Type type = jdbcTableHandle.getUpdatedColumnTypes().get(channel);
        Class<?> javaType = type.getJavaType();
        WriteFunction writeFunction = columnWriters.get(channel);
        if (javaType == boolean.class) {
            ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, type.getBoolean(block, position));
        }
        else if (javaType == long.class) {
            ((LongWriteFunction) writeFunction).set(statement, parameterIndex, type.getLong(block, position));
        }
        else if (javaType == double.class) {
            ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, type.getDouble(block, position));
        }
        else if (javaType == Slice.class) {
            ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, type.getSlice(block, position));
        }
        else if (javaType == Block.class) {
            ((BlockWriteFunction) writeFunction).set(statement, parameterIndex, (Block) type.getObject(block, position));
        }
        else {
            throw new VerifyException(format("Unexpected type %s with java type %s", type, javaType.getName()));
        }
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
    }

    private Map<String, String> getColumnNameMap(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        HashMap<String, String> columnNameMap = new HashMap<>(); //<columnName in lower case, columnName in datasource>
        List<JdbcColumnHandle> columnList = getColumns(session, tableHandle);

        for (JdbcColumnHandle columnHandle : columnList) {
            String columnName = columnHandle.getColumnName();
            columnNameMap.put(columnName.toLowerCase(ENGLISH), columnName);
        }

        return columnNameMap;
    }

    private List<String> getColumnNameFromDataSource(ConnectorSession session, JdbcTableHandle tableHandle, List<String> columns)
    {
        Map<String, String> columnNameMap = getColumnNameMap(session, tableHandle);
        List<String> updatedColumns = new ArrayList<>();

        for (String columnName : columns) {
            String originName = columnNameMap.get(columnName.toLowerCase(ENGLISH));
            updatedColumns.add((originName != null) ? originName : columnName);
        }

        return updatedColumns;
    }

    private String buildRemoteSchemaTableName(JdbcTableHandle tableHandle)
    {
        StringBuilder remoteSchemaTable = new StringBuilder();

        if (!isNullOrEmpty(tableHandle.getSchemaName())) {
            remoteSchemaTable.append(quoted(tableHandle.getSchemaName())).append(".");
        }

        remoteSchemaTable.append(quoted(tableHandle.getTableName()));

        return remoteSchemaTable.toString();
    }

    @Override
    public String buildUpdateSql(ConnectorSession session, ConnectorTableHandle handle, int setNum,
                                 List<String> updatedColumns)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) handle;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(format("UPDATE %s SET ", buildRemoteSchemaTableName(tableHandle)));
        List<String> columnList = getColumnNameFromDataSource(session, tableHandle, updatedColumns);
        for (int i = 0; i < setNum; i++) {
            sqlBuilder.append(quoted(columnList.get(i)));
            sqlBuilder.append(" = ? ");
            if (i != setNum - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append("WHERE " + "ctid" + "=?::tid");
        return sqlBuilder.toString();
    }

    @Override
    public void setUpdateSql(ConnectorSession session, ConnectorTableHandle tableHandle, PreparedStatement statement, List<Block> columnValueAndCtidBlock, int position, List<String> updatedColumns)
    {
        Block ctids = columnValueAndCtidBlock.get(columnValueAndCtidBlock.size() - 1);

        try {
            for (int i = 0; i < updatedColumns.size(); i++) {
                setStatement(session, tableHandle, statement, columnValueAndCtidBlock.get(i), position, i);
            }
            String ctid = ctids.getString(position, position, 10);
            statement.setString(updatedColumns.size() + 1, ctid);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        JdbcTypeHandle jdbcTypeHandle = new JdbcTypeHandle(Types.CHAR, Optional.of("char"), 10, 0, Optional.empty());
        return new JdbcColumnHandle("ctid", jdbcTypeHandle, VARCHAR, true);
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTypeHandle jdbcTypeHandle = new JdbcTypeHandle(Types.CHAR, Optional.of("char"), 10, 0, Optional.empty());
        return new JdbcColumnHandle("ctid", jdbcTypeHandle, VARCHAR, true);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        if (pushDownModule.equals(JdbcPushDownModule.DEFAULT)) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    private String extractSubQuery(String subQuery)
    {
        String query = subQuery.substring(subQuery.indexOf("WHERE"));
        int count = 1;
        int lastIndex = 0;
        for (int i = query.indexOf("(") + 1; i < query.length(); i++) {
            if (String.valueOf(query.charAt(i)).equals("(")) {
                count++;
            }
            if (String.valueOf(query.charAt(i)).equals(")")) {
                count--;
            }
            if (count == 0) {
                lastIndex = i;
                break;
            }
        }
        return query.substring(0, lastIndex + 1);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        JdbcTableHandle basePostgresqlHandle = (JdbcTableHandle) handle;
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = "DELETE FROM " + basePostgresqlHandle.getSchemaPrefixedTableName();
            if (basePostgresqlHandle.getGeneratedSql().isPresent()) {
                String subQuery = basePostgresqlHandle.getGeneratedSql().get().getSql();
                sql = String.format("%s %s", sql, extractSubQuery(subQuery));
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    return OptionalLong.of(statement.executeUpdate());
                }
            }
            else {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    log.debug("Execute: %s", sql);
                    return OptionalLong.of(statement.executeUpdate());
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;
        jdbcTableHandle.setDeleteOrUpdate(true);
        return jdbcTableHandle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
    }

    @Override
    public String buildDeleteSql(ConnectorTableHandle handle)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) handle;
        return format(
                "DELETE FROM %s WHERE ctid=%s", buildRemoteSchemaTableName(tableHandle), "?::tid");
    }

    @Override
    public void setDeleteSql(PreparedStatement statement, Block ctids, int position)
    {
        String ctid = ctids.getString(position, position, 10);
        try {
            statement.setString(1, ctid);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
