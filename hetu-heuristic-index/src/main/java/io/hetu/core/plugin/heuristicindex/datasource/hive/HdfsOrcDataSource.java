/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.heuristicindex.datasource.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.heuristicindex.DataSource;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.http.util.Asserts;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_BUCKET;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_CURRENT_TRANSACTION;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_OPERATION;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_ORIGINAL_TRANSACTION;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_ROW_ID;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_ROW_STRUCT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Comparator.comparingLong;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * HDFS ORC data source implementation
 * <p>
 * This datasource creates a split for every stripe.The split start will be the
 * byte offset where the stripe starts. This means, sorting the splits
 * by the split start will give the splits in the same order as the stripes.
 * Allowing you to get the data for a particular stripe based on the index
 * in the list.
 */
public class HdfsOrcDataSource
        implements DataSource
{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsOrcDataSource.class);

    private static final String ID = "ORC";
    private static final String ORC = "ORC";
    private static final int DEFAULT_MAX_ROWS_PER_SPLIT = 200000;
    private static final int DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors();
    /**
     * <pre>
     * ORC reader requires the {@link Type} object corresponding to the Hive column type.
     * We use the {@link TypeRegistry} for that. E.g. tinyint in hive corresponds to {@link TinyintType}
     *
     * Once the ORC reader reads the cell, it usually returns the corresponding
     * Java Primitive type. E.g. for {@link TinyintType} it returns a {@link Byte} object.
     *
     * In order to guarantee the accuracy of the Index filter we use, we need to limit the types
     * we can support. This is because when a query comes into hetu-main, the
     * type of the Expression is extracted from the Predicate. E.g. for tinyint,
     * if the expression is t_tinyint=123 the type of the Expression is GenericLiteral,
     * which needs to be converted from string to a byte object.(see
     * {@code PredicateExtractor#extract(Expression)}
     * in hetu-main).
     * We need to be able to convert the value in the predicate to the same type as which it was
     *  read using the ORC reader and stored in the Index. E.g. for tinyint both the ORC reader and
     *  PredicateExtractor convert the value to Byte. This allows us to guarantee the comparison.
     *
     * Below are the different types that Hive has and from the left how they are mapped to the Hetu Type object
     * and then the Java Primitive type. From the right how the PredicateExtractor converts to the same
     * Java Primitive type.
     *
     * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
     *
     * Numeric Types
     * tinyint -> {@link TinyintType} -> {@link Byte} <- {@link io.prestosql.sql.tree.GenericLiteral} (tinyint)
     * smallint -> {@link SmallintType} -> {@link Short} (y) <- {@link io.prestosql.sql.tree.GenericLiteral} (smallint)
     * integer -> {@link IntegerType} -> {@link Integer} (y) <- {@link io.prestosql.sql.tree.LongLiteral}
     * bigint -> {@link BigintType} -> {@link Long} (y) <- {@link io.prestosql.sql.tree.GenericLiteral} (bigint)
     * float/real -> {@link RealType} -> {@link Float} <- {@link io.prestosql.sql.tree.GenericLiteral} (real)
     * double -> {@link DoubleType} -> {@link Double} <- {@link io.prestosql.sql.tree.DoubleLiteral}
     * decimal -> {@link SqlDecimal} -> {@link java.math.BigDecimal} <- {@link io.prestosql.sql.tree.DoubleLiteral}
     * numeric??
     *
     * Date/Time Types
     * timestamp -> {@link io.prestosql.spi.type.TimestampType} -> {@link io.prestosql.spi.type.SqlTimestamp} <!--
     * -->(not supported)
     * date??
     * interval??
     *
     * String Types
     * string -> {@link VarcharType} -> {@link String} <- {@link io.prestosql.sql.tree.StringLiteral}
     * varchar(n) -> {@link VarcharType} -> {@link String} <- {@link io.prestosql.sql.tree.StringLiteral}
     * char(n) -> {@link CharType} -> {@link String} <- {@link io.prestosql.sql.tree.StringLiteral}
     *
     * Misc Types
     * boolean -> {@link BooleanType} -> {@link Boolean} <!--
     * --><- {@link io.prestosql.sql.tree.BooleanLiteral}
     * varbinary -> {@link io.prestosql.spi.type.VarbinaryType} -> {@link io.prestosql.spi.type.SqlVarbinary} <!--
     * -->(not supported)
     * </pre>
     */
    private static final Map<Class, String> SUPPORTED_TYPES; // Type to Hive column type

    private int concurrency;
    private int maxRowsPerSplit;

    private Configuration config;
    private Properties properties;
    private FileSystem fs;

    /**
     * Empty default constructor that does nothing (no field assignments)
     */
    public HdfsOrcDataSource()
    {
    }

    /**
     * Construct the HdfsOrcDataSource with the given Hive properties, Hadoop configuration, and Hadoop FileSystem
     * objects
     *
     * @param properties specific properties to this data source, e.g. concurrency limits, max rows per split.
     * @param configuration Hadoop configuration object
     * @param fs Hadoop filesystem object used to connect to HDFS
     */
    public HdfsOrcDataSource(Properties properties, Configuration configuration, FileSystem fs)
    {
        setProperties(properties);
        setConfiguration(configuration);
        setFs(fs);
    }

    /**
     * Check if a table has its data file in ORC format.
     *
     * @param metadata TableMetadata object for a specific table
     * @return true if the table's data storage format is ORC, otherwise false.
     */
    public static boolean isOrcTable(TableMetadata metadata)
    {
        String outputFormat = metadata.getOutputFormat();

        return outputFormat.toUpperCase(Locale.ENGLISH).contains(ORC);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    /**
     * <pre>
     * Validates the provided columns.
     *
     * Checks:
     * 1. column is not a partition column
     * 2. column is a table column
     * 3. column type is supported
     * </pre>
     *
     * @param tableMetadata target table's metadata object
     * @param columns columns to be validated
     * @return Immutable map containing each column's {@link HiveColumnHandle} and its type
     */
    private Map<HiveColumnHandle, Type> validateAndGetColumns(TableMetadata tableMetadata, String[] columns)
    {
        // convert columns names to lowercase for consistency
        Set<String> lcColumnNames = new HashSet<>(columns.length);
        for (String column : columns) {
            lcColumnNames.add(column.toLowerCase(ENGLISH));
        }

        Map<String, HiveColumnHandle> regularColumns = tableMetadata.getRegularColumns().stream()
                .collect(Collectors.toMap(
                        c -> c.getName().toLowerCase(ENGLISH),
                        Function.identity()));
        Map<HiveColumnHandle, Type> supportedColumns = tableMetadata.getSupportedColumnsWithTypeMapping();
        Map<HiveColumnHandle, Type> partitionColumns = tableMetadata.getPartitionColumns();

        // partition columns can not be indexed
        for (Map.Entry<HiveColumnHandle, Type
                > entry : partitionColumns.entrySet()) {
            if (lcColumnNames.contains(entry.getKey().getName().toLowerCase(ENGLISH))) {
                String msg = String.format("Partition columns can not be indexed: %s.",
                        entry.getKey().getName().toLowerCase(ENGLISH));
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        // create a mapping from column names to handles for searching
        Map<String, HiveColumnHandle> columnNameToHandle = supportedColumns.keySet().stream()
                .collect(Collectors.toMap(
                        hiveColumnHandle -> hiveColumnHandle.getName().toLowerCase(ENGLISH),
                        Function.identity()));

        ImmutableMap.Builder<HiveColumnHandle, Type> result = ImmutableMap.builder();

        for (String lcColumnName : lcColumnNames) {
            // ensure column name is valid
            if (!regularColumns.containsKey(lcColumnName)) {
                String msg = String.format(ENGLISH, "Column %s not found.", lcColumnName);
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }

            // ensure column type is supported
            // This checks the type that Hetu doesn't support
            Type columnType;
            if (!columnNameToHandle.containsKey(lcColumnName)) {
                String msg = String.format(ENGLISH, "Column %s with type %s is not supported",
                        lcColumnName, regularColumns.get(lcColumnName).getTypeSignature().toString());
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
            columnType = supportedColumns.get(columnNameToHandle.get(lcColumnName));
            // This checks the types that ORC reader cannot support
            // a column of type decimal(18,2) actually maps to ShortDecimalType
            // ShortDecimalType extends DecimalType but is package protected
            // so check if columnType is an instanceOf the supported types using cast
            // can't use instanceof or columnType.getClass.instanceOf()
            boolean isSupported = false;
            for (Class clazz : SUPPORTED_TYPES.keySet()) {
                try {
                    clazz.cast(columnType);
                }
                catch (ClassCastException e) {
                    continue;
                }
                isSupported = true;
                break;
            }

            if (!isSupported) {
                String msg = String.format("Indexing is not supported for column %s of type %s.",
                        lcColumnName, regularColumns.get(lcColumnName));
                LOG.error(msg);
                LOG.error("Supported types include: {}.", SUPPORTED_TYPES.values().toString());
                throw new IllegalArgumentException(msg);
            }

            result.put(regularColumns.get(lcColumnName), columnType);
        }

        return result.build();
    }

    @Override
    public void readSplits(String database, String table, String[] columns, String[] partitions, Callback callback)
            throws IOException
    {
        requireNonNull(columns, "no columns specified");

        // get table metadata
        TableMetadata tableMetadata = HadoopUtil.getTableMetadata(database, table, getProperties());

        // validate and get columns
        Map<HiveColumnHandle, Type> columnsMap = validateAndGetColumns(tableMetadata, columns);

        // column idx -> column type
        // required by the ORC reader later
        Map<Integer, Type> columnTypes = columnsMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().getHiveColumnIndex(),
                        Map.Entry::getValue));

        // column idx -> column name
        // required by the ORC reader later
        Map<Integer, String> columnNames = columnsMap.keySet().stream()
                .collect(Collectors.toMap(
                        HiveColumnHandle::getHiveColumnIndex,
                        column -> column.getName().toLowerCase(ENGLISH)));

        // search for all files in table dir
        String tableLocation = tableMetadata.getUri();
        Path tablePath = new Path(tableLocation);
        boolean isTransactional = AcidUtils.isTransactionalTable(tableMetadata.getTable().getParameters());
        boolean isFullAcid = AcidUtils.isFullAcidTable(tableMetadata.getTable().getParameters());
        List<FileStatus> files = HadoopUtil.getFiles(getFs(), tablePath, partitions, isTransactional);
        // schedule reading of each file
        ExecutorService executorServices = new ThreadPoolExecutor(getConcurrency(), getConcurrency(), 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(10240));
        try {
            List<Future> jobs = files.parallelStream().map(file -> executorServices.submit(() -> {
                        String path = file.getPath().toString();
                        long lastModified = file.getModificationTime();

                        FSDataInputStream in;
                        try {
                            in = getFs().open(new Path(path));
                        }
                        catch (Exception e) {
                            LOG.error(String.format(ENGLISH, "Unable to open file: %s. Skipping.", path), e);
                            return;
                        }

                        // HdfsOrcDataSource will close the inputstream
                        try (OrcDataSource source = new io.prestosql.plugin.hive.orc.HdfsOrcDataSource(
                                new OrcDataSourceId(path), file.getLen(),
                                new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE),
                                true, in, new FileFormatDataSourceStats())) {
                            readOrcFile(source, path, isFullAcid, columnTypes, columnNames, lastModified, callback);
                        }
                        catch (Exception e) {
                            LOG.error(String.format(ENGLISH, "Error reading file: %s. Skipping.", path), e);
                        }
                    }
            )).collect(Collectors.toList());

            for (Future jobFuture : jobs) {
                jobFuture.get();
            }
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
        finally {
            executorServices.shutdown();
        }
    }

    protected void readOrcFile(
            OrcDataSource source,
            String path,
            boolean isFullAcid,
            Map<Integer, Type> columnTypes,
            Map<Integer, String> columnNames,
            long lastModified,
            Callback callback)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(source, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE));

        Map<Integer, List<Object>> data = columnTypes.keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        key -> new LinkedList<>()));

        List<OrcColumn> fileColumns = orcReader.getRootColumn().getNestedColumns();
        List<OrcColumn> fileReadColumns = isFullAcid ? new ArrayList<>(columnNames.size() + 3) : new ArrayList<>(columnNames.size());
        List<Type> fileReadTypes = isFullAcid ? new ArrayList<>(columnNames.size() + 3) : new ArrayList<>(columnNames.size());
        if (isFullAcid) { // Skip the acid schema check in case of non-ACID files
            Map<String, OrcColumn> acidColumnsByName = uniqueIndex(fileColumns, OrcColumn::getColumnName);
            fileColumns = acidColumnsByName.get(ACID_COLUMN_ROW_STRUCT).getNestedColumns();

            fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ORIGINAL_TRANSACTION));
            fileReadTypes.add(BIGINT);
            fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_BUCKET));
            fileReadTypes.add(INTEGER);
            fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ROW_ID));
            fileReadTypes.add(BIGINT);
            fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_CURRENT_TRANSACTION));
            fileReadTypes.add(BIGINT);
            fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_OPERATION));
            fileReadTypes.add(INTEGER);
        }

        Map<OrcColumn, Integer> columnIdxMap = new HashMap<>();
        for (Map.Entry<Integer, Type> columnTypeEntry : columnTypes.entrySet()) {
            int index = columnTypeEntry.getKey();
            Type type = columnTypeEntry.getValue();
            fileReadColumns.add(fileColumns.get(index));
            fileReadTypes.add(type);
            columnIdxMap.put(fileColumns.get(index), index);
        }

        List<StripeInformation> stripes = orcReader.getFooter().getStripes().stream()
                .sorted(comparingLong(StripeInformation::getOffset))
                .collect(Collectors.toList());
        for (StripeInformation stripeInfo : stripes) {
            int rowCounter = 0;
            // Read the next stripe
            OrcRecordReader reader = orcReader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    OrcPredicate.TRUE,
                    stripeInfo.getOffset(),
                    stripeInfo.getDataLength(),
                    DateTimeZone.UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new);
            Page page;
            while ((page = reader.nextPage()) != null) {
                page = page.getLoadedPage();
                rowCounter += page.getPositionCount();
                // read current block
                for (int pos = 0; pos < fileReadColumns.size(); pos++) {
                    if (!columnIdxMap.containsKey(fileReadColumns.get(pos))) {
                        //Skip all columns except Index columns
                        continue;
                    }
                    Block block = page.getBlock(pos);
                    int columnIdx = columnIdxMap.get(fileReadColumns.get(pos));
                    Type type = columnTypes.get(columnIdx);
                    for (int position = 0; position < block.getPositionCount(); ++position) {
                        data.get(columnIdx).add(
                                getNativeValue(type, block, position));
                    }
                }
            }

            // finished reading current stripe
            if (rowCounter != stripeInfo.getNumberOfRows()) {
                throw new RuntimeException(String.format("Read rows %s did not match expected stripe rows %s",
                        rowCounter, stripeInfo.getNumberOfRows()));
            }

            // call callback method for current stripe
            for (Map.Entry<Integer, String> entry : columnNames.entrySet()) {
                List<Object> columnValues = data.get(entry.getKey());
                callback.call(
                        entry.getValue(),
                        columnValues.toArray(new Object[0]),
                        path,
                        stripeInfo.getOffset(),
                        lastModified);
                columnValues.clear();
            }
        }
    }

    protected Object getNativeValue(Type type, Block block, int position)
    {
        Object obj = TypeUtils.readNativeValue(type, block, position);
        Class<?> javaType = type.getJavaType();

        if (obj != null && javaType == Slice.class) {
            obj = ((Slice) obj).toStringUtf8();
        }

        return obj;
    }

    /**
     * Getter for Hadoop configuration object (lazy instantiation)
     *
     * @return the Hadoop configuration object. If it's called for the first time, the object will be newly generated
     * from the {@link #properties} object.
     * @throws IOException thrown when generating the Hadoop configuration
     */
    public Configuration getConfiguration()
            throws IOException
    {
        if (config == null) {
            config = HadoopUtil.generateHadoopConfig(getProperties());
        }

        return config;
    }

    /**
     * Setter for the configuration object
     *
     * @param newConfig Hadoop configuration object
     */
    public void setConfiguration(Configuration newConfig)
    {
        this.config = newConfig;
    }

    /**
     * Getter for properties object
     *
     * @return
     */
    @Override
    public Properties getProperties()
    {
        return properties;
    }

    /**
     * Setter for properties object
     *
     * @param properties The properties to be set
     */
    @Override
    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    /**
     * Getter for Hadoop FileSystem object (lazy instantiation)
     *
     * @return The filesystem object. If it's called for the first time, the object will be generated using the
     * {@link #config}
     * @throws IOException thrown when generating the FileSystem object or getting the Hadoop configuration object
     */
    public FileSystem getFs()
            throws IOException
    {
        if (fs == null) {
            fs = FileSystem.get(getConfiguration());
        }
        return fs;
    }

    public void setFs(FileSystem fs)
    {
        this.fs = fs;
    }

    /**
     * Getter for the maximum number of allowed threads for reading the ORC data sources
     * If {@link ConstantsHelper#HDFS_SOURCE_CONCURRENCY} is set from the configuration file, the value will be used
     * first, else the {@link #DEFAULT_CONCURRENCY} will be used
     *
     * @return the maximum number of allowed threads for reading the ORC data sources
     */
    public int getConcurrency()
    {
        if (concurrency == 0) {
            String maxThreads = properties.getProperty(ConstantsHelper.HDFS_SOURCE_CONCURRENCY);
            if (maxThreads != null) {
                concurrency = Integer.parseInt(maxThreads);
                Asserts.check(concurrency > 0, "maxThreads has to be greater than 0 in hive.properties");
            }
            else {
                concurrency = DEFAULT_CONCURRENCY;
            }
        }
        return concurrency;
    }

    /**
     * Sets the maximum number of the allowed threads for reading the ORC data sources
     *
     * @param concurrency maximum number of the allowed threads which has to be greater than 0
     */
    public void setConcurrency(int concurrency)
    {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("Concurrency has to be at least 1");
        }
        this.concurrency = concurrency;
    }

    static {
        ImmutableMap.Builder<Class, String> types = ImmutableMap.builder();
        types.put(TinyintType.class, "tinyint");
        types.put(SmallintType.class, "smallint");
        types.put(IntegerType.class, "integer");
        types.put(BigintType.class, "bigint");
        types.put(RealType.class, "real");
        types.put(DoubleType.class, "double");
        types.put(VarcharType.class, "varchar");
        types.put(CharType.class, "char");
        types.put(BooleanType.class, "boolean");
        types.put(DateType.class, "date");
        types.put(String.class, "string");
        SUPPORTED_TYPES = types.build();
    }
}
