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

package io.hetu.core.sql.migration.tool;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.airlift.airline.Command;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.hetu.core.sql.migration.SqlMigrationException;
import io.hetu.core.sql.migration.SqlSyntaxType;
import io.hetu.core.sql.migration.parser.Constants;
import io.prestosql.cli.CsvPrinter;
import io.prestosql.cli.LineReader;
import io.prestosql.cli.ThreadInterruptor;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.StatementSplitter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.inject.Inject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.prestosql.cli.Completion.commandCompleter;
import static io.prestosql.cli.CsvPrinter.CsvOutputFormat.NO_HEADER;
import static io.prestosql.sql.parser.StatementSplitter.squeezeStatement;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static jline.internal.Configuration.getUserHome;

@Command(name = "SqlMigration", description = "Presto sql migration tool")
public class Console
{
    private static final Logger log = Logger.get(Console.class);

    private static final int BUFFER_SIZE = 16384;
    private static final int HISTORY_SIZE = 10000;
    private static final String PROMPT_NAME = "lk";
    private static final String COMMAND_EXIT = "exit";
    private static final String COMMAND_QUIT = "quit";
    private static final String COMMAND_HISTORY = "history";
    private static final String COMMAND_HELP = "help";
    private static final Set<String> STATEMENT_SPLITTER = ImmutableSet.of(";", "\\G");
    private static final Duration EXIT_DELAY = new Duration(1, SECONDS);

    private static final Pattern HISTORY_INDEX_PATTERN = Pattern.compile("!\\d+");

    @Inject
    public CliOptions cliOptions = new CliOptions();

    public boolean run()
    {
        boolean hasQuery = cliOptions.execute != null;
        boolean isFromFile = !isNullOrEmpty(cliOptions.sqlFile);

        String query = cliOptions.execute;
        if (hasQuery) {
            query += ";";
        }

        if (isFromFile) {
            if (hasQuery) {
                System.out.println("Error: both --execute and --file specified");
                return false;
            }
            if (cliOptions.outputPath == null) {
                System.out.println("Error: --output not specified");
                return false;
            }

            try {
                query = asCharSource(new File(cliOptions.sqlFile), UTF_8).read();
                hasQuery = true;
            }
            catch (IOException e) {
                System.out.println(format("Error: Read failed from file %s: %s", cliOptions.sqlFile, e.getMessage()));
                return false;
            }
        }

        // abort any running query if the CLI is terminated
        AtomicBoolean exiting = new AtomicBoolean();
        ThreadInterruptor interruptor = new ThreadInterruptor();
        CountDownLatch exited = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            exiting.set(true);
            interruptor.interrupt();
            awaitUninterruptibly(exited, EXIT_DELAY.toMillis(), MILLISECONDS);
        }));

        // default source type is Hive
        SqlSyntaxType sourceType = SqlSyntaxType.HIVE;

        if (cliOptions.sourceType != null && !cliOptions.sourceType.isEmpty()) {
            switch (cliOptions.sourceType.toLowerCase(ENGLISH)) {
                case "hive":
                    sourceType = SqlSyntaxType.HIVE;
                    break;
                case "impala":
                    sourceType = SqlSyntaxType.IMPALA;
                    break;
                default:
                    System.out.println(format("Error: Migration tool doesn't support type: %s", cliOptions.sourceType));
                    return false;
            }
        }

        // get migration config
        MigrationConfig migrationConfig;
        try {
            migrationConfig = new MigrationConfig(cliOptions.configFile);
        }
        catch (IOException e) {
            System.out.println(format("Error: Read config file[%s] failed: %s", cliOptions.configFile, e.getMessage()));
            return false;
        }

        if (hasQuery) {
            // generate name of the target file
            String outputFile = null;
            if (cliOptions.outputPath != null) {
                if (!new File(cliOptions.outputPath).isDirectory()) {
                    System.out.println(format("Error: Output is not a directory: %s", cliOptions.outputPath));
                    return false;
                }

                outputFile = cliOptions.outputPath + File.separator;
                if (cliOptions.sqlFile == null) {
                    outputFile += System.currentTimeMillis();
                }
                else {
                    String fileName = new File(cliOptions.sqlFile).getName();
                    int dotIndex = fileName.lastIndexOf('.');
                    if (dotIndex == -1) {
                        outputFile += fileName + "_" + System.currentTimeMillis();
                    }
                    else {
                        outputFile += fileName.substring(0, dotIndex) + "_" + System.currentTimeMillis();
                    }
                }
            }
            return executeCommand(query, sourceType, outputFile, migrationConfig, cliOptions.execute != null);
        }

        runConsole(sourceType, exiting, migrationConfig);
        return true;
    }

    private void runConsole(SqlSyntaxType sourceType, AtomicBoolean exiting, MigrationConfig migrationConfig)
    {
        try (LineReader reader = new LineReader(getHistory(), commandCompleter())) {
            StringBuilder buffer = new StringBuilder();
            while (!exiting.get()) {
                // setup prompt
                String commandPrompt = PROMPT_NAME;
                if (sourceType != null) {
                    commandPrompt += ":" + sourceType.name();
                }

                // read a line of input from user
                if (buffer.length() > 0) {
                    commandPrompt = Strings.repeat(" ", commandPrompt.length() - 1) + "->";
                }
                else {
                    commandPrompt += ">";
                }
                String line = reader.readLine(commandPrompt);

                // add buffer to history and clear on user interrupt
                if (reader.interrupted()) {
                    if (!squeezeStatement(buffer.toString()).isEmpty()) {
                        reader.getHistory().add(squeezeStatement(buffer.toString()));
                    }
                    buffer = new StringBuilder();
                    continue;
                }

                // exit on EOF
                if (line == null) {
                    System.out.println("Exiting...");
                    System.out.println();
                    return;
                }

                // check for special commands if this is the first line
                if (buffer.length() == 0) {
                    String controlCommand = line.trim();

                    if (HISTORY_INDEX_PATTERN.matcher(controlCommand).matches()) {
                        int historyIndex = parseInt(controlCommand.substring(1));
                        History history = reader.getHistory();
                        if ((historyIndex <= 0) || (historyIndex > history.index())) {
                            System.err.println("The index number of history command does not exist.");
                            continue;
                        }
                        line = history.get(historyIndex - 1).toString();
                        System.out.println(commandPrompt + line);
                    }
                    else if (controlCommand.endsWith(";")) {
                        controlCommand = controlCommand.substring(0, controlCommand.length() - 1).trim();
                    }

                    switch (controlCommand.toLowerCase(ENGLISH)) {
                        case COMMAND_EXIT:
                        case COMMAND_QUIT:
                            System.out.println("Exiting...");
                            System.out.println();
                            return;
                        case COMMAND_HISTORY:
                            for (History.Entry entry : reader.getHistory()) {
                                System.out.printf("%5d  %s%n", entry.index() + 1, entry.value());
                            }
                            System.out.println();
                            System.out.printf("Choose and run the history command by index number. e.g. !10");
                            System.out.println();
                            continue;
                        case COMMAND_HELP:
                            System.out.println();
                            System.out.println(SqlMigrationHelp.getHelpText());
                            continue;
                    }
                }

                // not a command, add line to buffer
                buffer.append(line).append("\n");

                // execute any complete statements
                String sql = buffer.toString();
                StatementSplitter splitter = new StatementSplitter(sql, STATEMENT_SPLITTER);
                ConvertionOptions convertionOptions = new ConvertionOptions(sourceType, migrationConfig.isConvertDecimalAsDouble());
                SqlSyntaxConverter sqlConverter = SqlConverterFactory.getSqlConverter(convertionOptions);
                for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
                    reader.getHistory().add(squeezeStatement(split.statement()) + split.terminator());
                    JSONObject result = sqlConverter.convert(split.statement());
                    consolePrint(result);
                }

                // replace buffer with trailing partial statement
                buffer = new StringBuilder();
                if (!splitter.getPartialStatement().isEmpty()) {
                    buffer.append(splitter.getPartialStatement()).append('\n');
                }
            }
        }
        catch (IOException e) {
            System.out.println(format("Readline error: %s", e.getMessage()));
        }
        catch (ParsingException | SqlMigrationException e) {
            System.out.println(format("Failed to migrate the sql due to error: %s", e.getMessage()));
        }
    }

    private static void consolePrint(JSONObject result)
    {
        if (result == null) {
            return;
        }
        try {
            System.out.println();
            System.out.println("==========converted result==========");
            System.out.println(result.getString(Constants.CONVERTED_SQL));
            System.out.println(format("=================%s=============", result.getString(Constants.STATUS)));
            if (result.getString(Constants.MESSAGE).length() > 0) {
                System.out.println(result.getString(Constants.MESSAGE));
                System.out.println("====================================");
            }
            System.out.println();
        }
        catch (JSONException e) {
            System.out.println(format("Failed to convert sql due to:", e.getMessage()));
        }
    }

    private static MemoryHistory getHistory()
    {
        String historyFilePath = System.getenv("HETU_HISTORY_FILE");
        File historyFile;
        if (isNullOrEmpty(historyFilePath)) {
            historyFile = new File(getUserHome(), ".hetu_history");
        }
        else {
            historyFile = new File(historyFilePath);
        }
        return getHistory(historyFile);
    }

    private static MemoryHistory getHistory(File historyFile)
    {
        MemoryHistory history;
        try {
            createParentDirs(historyFile.getParentFile());
            historyFile.createNewFile();
            history = new FileHistory(historyFile);
            history.setMaxSize(HISTORY_SIZE);
        }
        catch (IOException e) {
            System.err.printf("Failed to load History file (%s): %s. ", historyFile, e.getMessage());
            System.out.printf("History will not be available during this session.%n");
            history = new MemoryHistory();
        }
        history.setAutoTrim(true);
        return history;
    }

    private boolean executeCommand(String query, SqlSyntaxType sourceType, String outputFile, MigrationConfig migrationConfig, boolean isConsolePrintEnable)
    {
        JSONArray output = new JSONArray();
        try {
            StatementSplitter splitter = new StatementSplitter(query);
            ConvertionOptions convertionOptions = new ConvertionOptions(sourceType, migrationConfig.isConvertDecimalAsDouble());
            SqlSyntaxConverter sqlConverter = SqlConverterFactory.getSqlConverter(convertionOptions);
            for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
                JSONObject result = sqlConverter.convert(split.statement());
                output.put(result);
                if (isConsolePrintEnable) {
                    consolePrint(result);
                }
            }
            log.info(format("Migration Completed."));

            // write the result to file if output file was specified
            if (outputFile != null) {
                writeToSqlFile(output, outputFile);
                writeToCsvFile(output, outputFile);
            }
            return true;
        }
        catch (ParsingException | SqlMigrationException e) {
            log.error(format("Failed to migrate the sql due to error:", e.getMessage()));
        }
        return false;
    }

    private static boolean writeToSqlFile(JSONArray jsonArray, String outputFile)
    {
        BufferedWriter writer = null;
        try {
            OutputStream out = new FileOutputStream(outputFile + ".sql");
            writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8), BUFFER_SIZE);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject result = jsonArray.getJSONObject(i);
                if (!result.get(Constants.STATUS).toString().equals(Constants.FAIL)) {
                    writer.write(result.get(Constants.CONVERTED_SQL).toString() + ";");
                    writer.newLine();
                }
            }
        }
        catch (JSONException | IOException e) {
            log.error(format("Failed to save the convert result as a sql file"));
        }
        finally {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    log.error(format("Write file[%s.sql] failed: %s", outputFile, e.getMessage()));
                }
            }
        }
        log.info(format("Result is saved to %s.sql", outputFile));
        return true;
    }

    private static boolean writeToCsvFile(JSONArray jsonArray, String outputFile)
    {
        // create an file output stream
        BufferedWriter writer = null;
        try {
            OutputStream out = new FileOutputStream(outputFile + ".csv");
            writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8), BUFFER_SIZE);
            CsvPrinter csvPrinter = new CsvPrinter(new ArrayList<>(), writer, NO_HEADER);
            List<List<String>> csvData = jsonToCsv(jsonArray, true);
            for (List<String> row : csvData) {
                csvPrinter.printRows(Collections.singletonList(row), false);
            }
            csvPrinter.finish();
        }
        catch (JSONException | IOException e) {
            log.error(format("Failed to save the convert result as a csv file"));
        }
        finally {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    log.error(format("Write file[%s.csv] failed: %s", outputFile, e.getMessage()));
                }
            }
        }
        log.info(format("Result is saved to %s.csv", outputFile));
        return true;
    }

    private static List<List<String>> jsonToCsv(JSONArray jsonArray, boolean isWithHeader) throws JSONException
    {
        List<List<String>> csvRows = new ArrayList<>(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject row = jsonArray.getJSONObject(i);

            // add header
            if (isWithHeader) {
                Iterator<String> keys = row.keys();
                List<String> header = new ArrayList<>();
                while (keys.hasNext()) {
                    header.add(keys.next());
                }
                csvRows.add(header);
                isWithHeader = false;
            }

            // add data row
            List<String> dataRow = new ArrayList<>();
            Iterator<String> keys = row.keys();
            while (keys.hasNext()) {
                dataRow.add(row.getString(keys.next()));
            }
            csvRows.add(dataRow);
        }

        return csvRows;
    }
}
