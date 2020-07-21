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

package io.hetu.core.heuristicindex;

import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.heuristicindex.IndexCommandUtils.loadDataSourceProperties;
import static io.hetu.core.heuristicindex.IndexCommandUtils.loadIndexProperties;
import static io.hetu.core.heuristicindex.IndexCommandUtils.loadIndexStore;
import static java.util.Objects.requireNonNull;

/**
 * Entry class for indexer
 */
@CommandLine.Command(name = "index",
        description =
                "\nThe Heuristic Indexer allows creating indexes on existing data and stores the index external to the original data source." +
                        "This means existing data can be indexed without having to rewrite the source data files. " +
                        "And new index types not supported by the underlying data source can be created. " +
                        "\n\nCurrently the Hetu Engine can utilize the indexes created by the Heuristic Indexer to perform " +
                        "filtering (using BLOOM or MINMAX Indexes) while scheduling splits. " +
                        "The engine can also perform filtering while reading ORC files (using the BITMAP Index). \n\n" +
                        "Using this index tool, you can CREATE, SHOW and DELETE indexes. \n\n" +
                        "Supported index types: BITMAP, BLOOM, MINMAX\n\n" +
                        "Supported index stores: LOCAL, HDFS (must be configured in {--config}/config.properties\n\n" +
                        "Supported data sources: HIVE (must be configured in {--config}/catalog/catalog_name.properties\n\n" +
                        "Notes on resource usage:\n\n" +
                        "By default the default JVM MaxHeapSize will be used (java -XX:+PrintFlagsFinal -version | grep MaxHeapSize).\n" +
                        "For improved performance, it is recommended to increase the MaxHeapSize. This can be done by setting -Xmx value:\n\n" +
                        "export JAVA_TOOL_OPTIONS=\"-Xmx100G\"\n\n" +
                        "in this example the MaxHeapSize will be set to 100G.\n\n" +
                        "If creating the index for a large table is too slow on one machine, you can create index for different partitions in " +
                        "parallel on different machines.  This requires setting the --disableLocking flag.\n\n" +
                        "For example:\n\n" +
                        "One machine 1:\n" +
                        "$ ./index -v ---disableLocking c ../etc --table hive.schema.table --column column1,column2 --type bloom,minmax,bitmap --partition p=part1 create\n\n" +
                        "One machine 2:\n" +
                        "$ ./index -v --disableLocking -c ../etc --table hive.schema.table --column column1,column2 --type bloom,minmax,bitmap --partition p=part2 create\n\n" +
                        "Examples:\n\n" +
                        "1) Create index\n\n" +
                        "$ ./index -v -c ../etc --table hive.schema.table --column column1,column2 --type bloom,minmax,bitmap --partition p=part1 create\n\n" +
                        "2) Show index\n\n" +
                        "$ ./index -v -c ../etc --table hive.schema.table show\n\n" +
                        "3) Delete index (Note: index can only be deleted at table or column level, i.e. all index types will be deleted)\n\n" +
                        "$ ./index -v -c ../etc --table hive.schema.table --column column1 delete\n\n" +
                        "")
public class IndexCommand
        implements Callable<Void>
{
    private static final Logger LOG = LoggerFactory.getLogger(IndexCommand.class);
    @CommandLine.Option(
            names = {"-c", "--config"},
            required = true,
            defaultValue = "../etc",
            description = "root folder of hetu etc directory (default: ${DEFAULT-VALUE})")
    String configDirPath;
    @CommandLine.Option(
            names = {"-t", "--table"},
            required = true,
            description = "fully qualified table name")
    String table;
    @CommandLine.Option(
            names = {"-C", "--column"},
            split = ",",
            description = "column, comma separated format for multiple columns")
    String[] columns;
    @CommandLine.Option(
            names = {"-P", "--partition"},
            split = ",",
            description = "only create index for these partitions, comma separated format for multiple partitions")
    String[] partitions;
    @CommandLine.Option(
            names = {"-T", "--type"},
            split = ",",
            description = "index type, comma separated format for multiple types (supported types: BLOOM, BITMAP, MINMAX")
    String[] indexTypes;
    @CommandLine.Parameters(
            index = "0",
            description = "command types, e.g. create, delete, show;" +
                    " delete command works a column level")
    Command command;
    @CommandLine.Option(
            names = {"-L", "--disableLocking"},
            description = "by default locking is enabled at the table level; if this is set to false, the user must ensure " +
                    "that the same data is not indexed by multiple callers at the same time (indexing different columns " +
                    "or partitions in parallel is allowed)")
    boolean disableLocking;
    @CommandLine.Option(
            names = {"-d", "--debug"},
            description = "if debug is enabled the original data for each split will also be written to a file " +
                    "alongside the index")
    boolean debugEnabled; // disabled by default
    @CommandLine.Option(
            names = {"-v", "--verbose"},
            description = "verbose")
    boolean verbose; // disabled by default

    IndexCommand()
    {
    }

    /**
     * start application
     *
     * @param args args from commandline
     */
    public static void main(String[] args)
    {
        CommandLine commandLine = new CommandLine(new IndexCommand());
        commandLine.setUnmatchedArgumentsAllowed(Boolean.TRUE);
        commandLine.execute(args);
    }

    @Override
    public Void call()
            throws IOException
    {
        // make sure the file paths provided exist
        checkArgument(Paths.get(configDirPath).toFile().exists(), "Config directory does not exist");

        IndexFactory factory = IndexCommandUtils.getIndexFactory();

        // based on the command, different values are required
        try {
            Properties dsProperties = loadDataSourceProperties(table, configDirPath);
            Properties ixProperties = loadIndexProperties(configDirPath);
            IndexCommandUtils.IndexStore indexStore = loadIndexStore(configDirPath);
            switch (command) {
                case create:
                    requireNonNull(indexTypes, "No index type specified for create command");
                    requireNonNull(columns, "No columns specified for create command");
                    IndexWriter writer = factory.getIndexWriter(dsProperties, ixProperties, indexStore.getFs(), indexStore.getRoot());
                    writer.createIndex(table, columns, partitions, indexTypes, !disableLocking, debugEnabled);
                    break;
                case delete:
                    IndexClient deleteClient = factory.getIndexClient(indexStore.getFs(), indexStore.getRoot());
                    deleteClient.deleteIndex(table, columns);
                    break;
                case show:
                    IndexClient showClient = factory.getIndexClient(indexStore.getFs(), indexStore.getRoot());
                    List<IndexMetadata> indexes = showClient.readSplitIndex(table);
                    indexes.stream()
                            .sorted(Comparator
                                    .comparing(IndexMetadata::getTable)
                                    .thenComparing(IndexMetadata::getColumn)
                                    .thenComparing(i -> i.getIndex().getId())
                                    .thenComparing(IndexMetadata::getUri)
                                    .thenComparing(IndexMetadata::getSplitStart))
                            .forEach(System.out::println);
                    break;
                default:
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Command [%s] is not supported by the indexer", command));
            }
        }
        catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error occurred, please check the stacktrace for details: ", e);
            }
            else {
                LOG.info("Error occurred. Enabled -v option for more details. %s", e.getMessage());
            }
        }

        return null;
    }

    /**
     * Enumeration for different Commands that indexer supports
     */
    enum Command
    {
        create,
        delete,
        show
    }
}
