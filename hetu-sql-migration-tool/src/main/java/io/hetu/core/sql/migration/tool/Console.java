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
import io.hetu.core.sql.migration.Constants;
import io.hetu.core.sql.migration.SqlMigrationException;
import io.hetu.core.sql.migration.SqlSyntaxType;
import io.hetu.core.sql.util.SqlResultHandleUtils;
import io.prestosql.cli.InputReader;
import io.prestosql.cli.ThreadInterruptor;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.StatementSplitter;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jline.reader.History;
import org.jline.reader.UserInterruptException;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.google.common.base.StandardSystemProperty.USER_HOME;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.hetu.core.sql.migration.Constants.HIVE;
import static io.hetu.core.sql.migration.Constants.IMPALA;
import static io.prestosql.cli.Completion.commandCompleter;
import static io.prestosql.sql.parser.StatementSplitter.squeezeStatement;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Command(name = "SqlMigration", description = "Presto sql migration tool")
public class Console
{
    private static final Logger log = Logger.get(Console.class);

    private static final String PROMPT_NAME = "lk";
    private static final String COMMAND_EXIT = "exit";
    private static final String COMMAND_QUIT = "quit";
    private static final String COMMAND_HISTORY = "history";
    private static final String COMMAND_HELP = "help";
    private static final Set<String> STATEMENT_SPLITTER = ImmutableSet.of(";", "\\G");
    private static final Duration EXIT_DELAY = new Duration(1, SECONDS);

    private static final Pattern SET_COMMAND_PATTERN = Pattern.compile("![\\w ]+");
    private static final String CHANGE_TYPE_COMMAND = "chtype";

    @Inject
    public CliOptions cliOptions = new CliOptions();

    public boolean run()
    {
        SessionProperties session = new SessionProperties();

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
                System.out.println(format("Error: Read failed from input file %s", e.getMessage()));
                return false;
            }
        }

        // set source type
        if (cliOptions.sourceType != null && !cliOptions.sourceType.isEmpty()) {
            if (!setSourceType(cliOptions.sourceType.toLowerCase(ENGLISH), session)) {
                System.out.println("Error: Migration tool doesn't support the sql type you choose");
                return false;
            }
        }
        else {
            // by default, set source type to hive
            setSourceType(HIVE, session);
        }

        // get migration config
        try {
            session.setMigrationConfig(new MigrationConfig(cliOptions.configFile));
        }
        catch (IOException e) {
            System.out.println(format("Error: Read config file failed: %s", e.getMessage()));
            return false;
        }

        try (ThreadInterruptor interruptor = new ThreadInterruptor()) {
            // abort any running query if the CLI is terminated
            AtomicBoolean exiting = new AtomicBoolean();
            CountDownLatch exited = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                exiting.set(true);
                interruptor.interrupt();
                awaitUninterruptibly(exited, EXIT_DELAY.toMillis(), MILLISECONDS);
            }));

            if (hasQuery) {
                // generate name of the target file
                String outputFile = null;
                if (cliOptions.outputPath != null) {
                    if (!new File(cliOptions.outputPath).isDirectory()) {
                        System.out.println("Error: Output is not a directory");
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
                    if (new File(outputFile).exists()) {
                        System.out.println(format("Error: Output file %s already exists!", outputFile));
                        return false;
                    }
                }
                session.setDebugEnable(cliOptions.debug != null && cliOptions.debug.equalsIgnoreCase("true"));
                session.setConsolePrintEnable(cliOptions.execute != null || session.isDebugEnable());
                return executeCommand(query, outputFile, session);
            }

            runConsole(session, exiting);
            return true;
        }
    }

    private void runConsole(SessionProperties session, AtomicBoolean exiting)
    {
        try (InputReader reader = new InputReader(getHistoryFile(), commandCompleter())) {
            StringBuilder buffer = new StringBuilder();
            while (!exiting.get()) {
                // setup prompt
                String commandPrompt = PROMPT_NAME;
                if (session.getSourceType() != null) {
                    commandPrompt += ":" + session.getSourceType().name();
                }

                // read a line of input from user
                if (buffer.length() > 0) {
                    commandPrompt = Strings.repeat(" ", commandPrompt.length() - 1) + "->";
                }
                else {
                    commandPrompt += ">";
                }
                String line;
                try {
                    line = reader.readLine(commandPrompt, buffer.toString());
                }
                catch (UserInterruptException e) {
                    // add buffer to history and clear on user interrupt
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

                    if (controlCommand.endsWith(";")) {
                        controlCommand = controlCommand.substring(0, controlCommand.length() - 1).trim();
                        if (SET_COMMAND_PATTERN.matcher(controlCommand).matches()) {
                            controlCommand = controlCommand.replaceAll(" +", " ");
                            setSessionProperties(controlCommand.replace("!", ""), session);
                            continue;
                        }
                    }

                    switch (controlCommand.toLowerCase(ENGLISH)) {
                        case COMMAND_EXIT:
                        case COMMAND_QUIT:
                            System.out.println("Exiting...");
                            System.out.println();
                            return;
                        case COMMAND_HISTORY:
                            for (History.Entry entry : reader.getHistory()) {
                                System.out.printf("%5d  %s%n", entry.index() + 1, entry.line());
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
                SqlSyntaxConverter sqlConverter = SqlConverterFactory.getSqlConverter(session);
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

    private boolean setSourceType(String sourceType, SessionProperties sessionProperties)
    {
        if (sourceType == null || sourceType.isEmpty()) {
            return false;
        }
        switch (sourceType.toLowerCase(ENGLISH)) {
            case HIVE:
                sessionProperties.setSourceType(SqlSyntaxType.HIVE);
                break;
            case IMPALA:
                sessionProperties.setSourceType(SqlSyntaxType.IMPALA);
                break;
            default:
                return false;
        }
        return true;
    }

    private void setSessionProperties(String controlCommand, SessionProperties session)
    {
        if (controlCommand == null || controlCommand.isEmpty()) {
            return;
        }

        String[] commandStrs = controlCommand.split(" ");

        switch (commandStrs[0].toLowerCase(ENGLISH)) {
            case CHANGE_TYPE_COMMAND:
                if (commandStrs.length != 2) {
                    System.out.println("You have to specify the input sql type. e.g. !chtype hive");
                    break;
                }
                String targetType = commandStrs[1].toLowerCase(ENGLISH);
                if (!setSourceType(targetType, session)) {
                    System.out.println(format("Unable to change type to %s. Because it is invalid or unsupported.", targetType));
                }
                break;
            default:
                System.out.println(format("Command %s is not supported", commandStrs[0]));
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

    private static Path getHistoryFile()
    {
        String path = System.getenv("MIGRATION_TOOL_HISTORY_FILE");
        if (!isNullOrEmpty(path)) {
            return Paths.get(path);
        }
        return Paths.get(nullToEmpty(USER_HOME.value()), ".migration_tool_history");
    }

    public boolean executeCommand(String query, String outputFile, SessionProperties session)
    {
        JSONArray output = new JSONArray();
        try {
            StatementSplitter splitter = new StatementSplitter(query);
            SqlSyntaxConverter sqlConverter = SqlConverterFactory.getSqlConverter(session);
            for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
                if (session.isDebugEnable()) {
                    log.info(String.format("Processing sql: %s", split.toString()));
                }
                JSONObject result = sqlConverter.convert(split.statement());
                output.put(result);
                if (session.isConsolePrintEnable()) {
                    consolePrint(result);
                }
            }
            log.info(format("Migration Completed."));

            // write the result to file if output file was specified
            if (outputFile != null) {
                SqlResultHandleUtils.writeToHtmlFile(output, outputFile);
            }
            return true;
        }
        catch (ParsingException | SqlMigrationException e) {
            log.error(format("Failed to migrate the sql due to error:", e.getMessage()));
        }
        return false;
    }
}
