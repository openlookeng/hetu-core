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
package io.prestosql.cli;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.units.Duration;
import io.prestosql.client.ClientSelectedRole;
import io.prestosql.client.ClientSession;
import io.prestosql.sql.parser.StatementSplitter;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.StandardSystemProperty.USER_HOME;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.prestosql.cli.Help.getHelpText;
import static io.prestosql.cli.QueryPreprocessor.preprocessQuery;
import static io.prestosql.client.ClientSession.stripTransactionId;
import static io.prestosql.sql.parser.StatementSplitter.Statement;
import static io.prestosql.sql.parser.StatementSplitter.isEmptyStatement;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jline.terminal.TerminalBuilder.terminal;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;

@Command(name = "openLooKeng", description = "openLooKeng interactive console")
public class Console
{
    public static final Set<String> STATEMENT_DELIMITERS = ImmutableSet.of(";", "\\G");

    private static final String PROMPT_NAME = "lk";
    private static final Duration EXIT_DELAY = new Duration(3, SECONDS);
    private static final Pattern CREATE_CUBE_PATTERN = Pattern.compile("^(create)\\s*cube.*where.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private static final Pattern RELOAD_CUBE_PATTERN = Pattern.compile("^(reload)\\s*cube.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    @Inject
    public HelpOption helpOption;

    @Inject
    public VersionOption versionOption = new VersionOption();

    @Inject
    public ClientOptions clientOptions = new ClientOptions();

    public boolean run()
    {
        ClientSession session = clientOptions.toClientSession();
        boolean hasQuery = !isNullOrEmpty(clientOptions.execute);
        boolean isFromFile = !isNullOrEmpty(clientOptions.file);
        initializeLogging(clientOptions.logLevelsFile);

        String query = clientOptions.execute;
        if (hasQuery) {
            query += ";";
        }

        if (isFromFile) {
            if (hasQuery) {
                throw new RuntimeException("both --execute and --file specified");
            }
            try {
                query = asCharSource(new File(clientOptions.file), UTF_8).read();
                hasQuery = true;
            }
            catch (IOException e) {
                throw new RuntimeException(format("Error reading from file %s: %s", clientOptions.file, e.getMessage()));
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

        try (QueryRunner queryRunner = new QueryRunner(
                session,
                clientOptions.debug,
                Optional.ofNullable(clientOptions.socksProxy),
                Optional.ofNullable(clientOptions.httpProxy),
                Optional.ofNullable(clientOptions.keystorePath),
                Optional.ofNullable(clientOptions.keystorePassword),
                Optional.ofNullable(clientOptions.truststorePath),
                Optional.ofNullable(clientOptions.truststorePassword),
                Optional.ofNullable(clientOptions.accessToken),
                Optional.ofNullable(getUser()),
                clientOptions.password ? Optional.of(getPassword()) : Optional.empty(),
                Optional.ofNullable(clientOptions.krb5Principal),
                Optional.ofNullable(clientOptions.krb5ServicePrincipalPattern),
                Optional.ofNullable(clientOptions.krb5RemoteServiceName),
                Optional.ofNullable(clientOptions.krb5ConfigPath),
                Optional.ofNullable(clientOptions.krb5KeytabPath),
                Optional.ofNullable(clientOptions.krb5CredentialCachePath),
                !clientOptions.krb5DisableRemoteServiceHostnameCanonicalization)) {
            if (hasQuery) {
                return executeCommand(
                        queryRunner,
                        exiting,
                        query,
                        clientOptions.outputFormat,
                        clientOptions.ignoreErrors,
                        clientOptions.progress);
            }
            runConsole(queryRunner, exiting);
            return true;
        }
        finally {
            exited.countDown();
            interruptor.close();
        }
    }

    private String getUser()
    {
        if (clientOptions.user != null) {
            return clientOptions.user;
        }
        if (clientOptions.krb5Principal != null) {
            return "";
        }
        return System.getProperty("user.name");
    }

    private String getPassword()
    {
        checkState(clientOptions.user != null, "Username must be specified along with password");
        String defaultPassword = System.getenv("OPENLOOKENG_PASSWORD");
        if (defaultPassword != null) {
            return defaultPassword;
        }

        java.io.Console console = System.console();
        if (console != null) {
            char[] password = console.readPassword("Password: ");
            if (password != null) {
                return new String(password);
            }
            return "";
        }
        try (Terminal terminal = terminal()) {
            org.jline.reader.LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
            return reader.readLine("Password: ", (char) 0);
        }
        catch (EndOfFileException e) {
            return "";
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read password from console", exception);
        }
    }

    public ClientOptions getClientOptions()
    {
        return clientOptions;
    }

    private void runConsole(QueryRunner queryRunner, AtomicBoolean exiting)
    {
        try (TableNameCompleter tableNameCompleter = new TableNameCompleter(queryRunner);
                InputReader reader = new InputReader(getHistoryFile(), Completion.commandCompleter(), tableNameCompleter)) {
            tableNameCompleter.populateCache();
            String remaining = "";
            while (!exiting.get()) {
                // setup prompt
                String prompt = PROMPT_NAME;
                String schema = queryRunner.getSession().getSchema();
                if (schema != null) {
                    prompt += ":" + schema.replace("%", "%%");
                }
                String commandPrompt = prompt + "> ";

                // read a line of input from user
                String line;
                try {
                    line = reader.readLine(commandPrompt, remaining);
                }
                catch (UserInterruptException e) {
                    if (!e.getPartialLine().isEmpty()) {
                        reader.getHistory().add(e.getPartialLine());
                    }
                    remaining = "";
                    continue;
                }
                catch (EndOfFileException e) {
                    System.out.println();
                    return;
                }

                // check for special commands -- must match InputParser
                String command = CharMatcher.is(';').or(whitespace()).trimTrailingFrom(line);
                switch (command.toLowerCase(ENGLISH)) {
                    case "exit":
                    case "quit":
                        return;
                    case "history":
                        for (History.Entry entry : reader.getHistory()) {
                            System.out.println(new AttributedStringBuilder()
                                    .style(DEFAULT.foreground(CYAN))
                                    .append(format("%5d", entry.index() + 1))
                                    .style(DEFAULT)
                                    .append("  ")
                                    .append(entry.line())
                                    .toAnsi(reader.getTerminal()));
                        }
                        continue;
                    case "help":
                        System.out.println();
                        System.out.println(getHelpText());
                        continue;
                    default:
                        break;
                }
                // execute any complete statements
                StatementSplitter splitter = new StatementSplitter(line, STATEMENT_DELIMITERS);
                for (Statement split : splitter.getCompleteStatements()) {
                    ClientOptions.OutputFormat outputFormat = ClientOptions.OutputFormat.ALIGNED;
                    if (split.terminator().equals("\\G")) {
                        outputFormat = ClientOptions.OutputFormat.VERTICAL;
                    }
                    if (CREATE_CUBE_PATTERN.matcher(split.statement()).matches()) {
                        PrintStream out = System.out;
                        PrintStream errorChannel = System.out;
                        CubeConsole cubeConsole = new CubeConsole(this);
                        queryRunner.setCubeConsole(cubeConsole);
                        boolean isCreateCubeSucceed = cubeConsole.createCubeCommand(split.statement(), queryRunner, ClientOptions.OutputFormat.NULL, tableNameCompleter::populateCache, false, true, reader.getTerminal(), out, errorChannel);
                    }
                    else if (RELOAD_CUBE_PATTERN.matcher(split.statement()).matches()) {
                        PrintStream out = System.out;
                        PrintStream errorChannel = System.out;
                        ReloadCubeConsole reloadCubeConsole = new ReloadCubeConsole(this);
                        queryRunner.setReloadCubeConsole(reloadCubeConsole);
                        boolean isReloadCubeSucceed = reloadCubeConsole.reload(split.statement(), queryRunner, ClientOptions.OutputFormat.VERTICAL, tableNameCompleter::populateCache, true, true, reader.getTerminal(), out, errorChannel);
                        if (isReloadCubeSucceed) {
                            CubeConsole cubeConsole = new CubeConsole(this);
                            queryRunner.setCubeConsole(cubeConsole);
                            if (CREATE_CUBE_PATTERN.matcher(reloadCubeConsole.getNewQuery()).matches()) {
                                boolean isCreateCubeSucceed = cubeConsole.createCubeCommand(reloadCubeConsole.getNewQuery(), queryRunner, ClientOptions.OutputFormat.NULL, tableNameCompleter::populateCache, false, true, reader.getTerminal(), out, errorChannel);
                                if (!isCreateCubeSucceed) {
                                    System.out.println("An Error occurred. The cube needs to be created manually. Query to create the cube: " + reloadCubeConsole.getNewQuery());
                                }
                            }
                            else {
                                process(queryRunner, reloadCubeConsole.getNewQuery(), outputFormat, tableNameCompleter::populateCache, true, true, reader.getTerminal(), System.out, System.out);
                            }
                        }
                    }
                    else {
                        process(queryRunner, split.statement(), outputFormat, tableNameCompleter::populateCache, true, true, reader.getTerminal(), System.out, System.out);
                    }
                }
                // replace remaining with trailing partial statement
                remaining = whitespace().trimTrailingFrom(splitter.getPartialStatement());
            }
        }
        catch (IOException e) {
            System.err.println("Readline error: " + e.getMessage());
        }
    }

    private boolean executeCommand(
            QueryRunner queryRunner,
            AtomicBoolean exiting,
            String query,
            ClientOptions.OutputFormat outputFormat,
            boolean ignoreErrors,
            boolean showProgress)
    {
        boolean success = true;
        StatementSplitter splitter = new StatementSplitter(query);
        for (Statement split : splitter.getCompleteStatements()) {
            if (!isEmptyStatement(split.statement())) {
                try (Terminal terminal = terminal()) {
                    String statement = split.statement();
                    CubeConsole cubeConsole = new CubeConsole(this);
                    queryRunner.setCubeConsole(cubeConsole);
                    ReloadCubeConsole reloadCubeConsole = new ReloadCubeConsole(this);
                    queryRunner.setReloadCubeConsole(reloadCubeConsole);
                    if (CREATE_CUBE_PATTERN.matcher(statement).matches()) {
                        PrintStream out = System.out;
                        PrintStream errorChannel = System.out;
                        if (!cubeConsole.createCubeCommand(statement, queryRunner, ClientOptions.OutputFormat.NULL, () -> {}, false, showProgress, terminal, out, errorChannel)) {
                            if (!ignoreErrors) {
                                return false;
                            }
                            success = false;
                        }
                    }
                    else if (RELOAD_CUBE_PATTERN.matcher(statement).matches()) {
                        PrintStream out = System.out;
                        PrintStream errorChannel = System.out;
                        boolean isReloadCubeSucceed = reloadCubeConsole.reload(statement, queryRunner, ClientOptions.OutputFormat.NULL, () -> {}, false, showProgress, terminal, out, errorChannel);
                        if (!isReloadCubeSucceed) {
                            if (!ignoreErrors) {
                                return false;
                            }
                            success = false;
                        }
                        else {
                            if (CREATE_CUBE_PATTERN.matcher(reloadCubeConsole.getNewQuery()).matches()) {
                                boolean isCreateCubeSucceed = cubeConsole.createCubeCommand(reloadCubeConsole.getNewQuery(), queryRunner, ClientOptions.OutputFormat.NULL, () -> {}, false, showProgress, terminal, out, errorChannel);
                                if (!isCreateCubeSucceed) {
                                    System.err.println("An Error occurred. The cube needs to be created manually. Query to create the cube: " + reloadCubeConsole.getNewQuery());
                                    if (!ignoreErrors) {
                                        return false;
                                    }
                                    success = false;
                                }
                            }
                            else {
                                if (!process(queryRunner, reloadCubeConsole.getNewQuery(), outputFormat, () -> {}, false, showProgress, terminal, System.out, System.err)) {
                                    if (!ignoreErrors) {
                                        return false;
                                    }
                                    success = false;
                                }
                            }
                        }
                    }
                    else {
                        if (!process(queryRunner, statement, outputFormat, () -> {}, false, showProgress, terminal, System.out, System.err)) {
                            if (!ignoreErrors) {
                                return false;
                            }
                            success = false;
                        }
                    }
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            if (exiting.get()) {
                return success;
            }
        }
        if (!isEmptyStatement(splitter.getPartialStatement())) {
            System.err.println("Non-terminated statement: " + splitter.getPartialStatement());
            return false;
        }
        return success;
    }

    private static boolean process(
            QueryRunner queryRunner,
            String sql,
            ClientOptions.OutputFormat outputFormat,
            Runnable schemaChanged,
            boolean usePager,
            boolean showProgress,
            Terminal terminal,
            PrintStream out,
            PrintStream errorChannel)
    {
        String finalSql;
        try {
            finalSql = preprocessQuery(
                    Optional.ofNullable(queryRunner.getSession().getCatalog()),
                    Optional.ofNullable(queryRunner.getSession().getSchema()),
                    sql);
        }
        catch (QueryPreprocessorException e) {
            System.err.println(e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace(System.err);
            }
            return false;
        }

        try (Query query = queryRunner.startQuery(finalSql)) {
            boolean success = query.renderOutput(terminal, out, errorChannel, outputFormat, usePager, showProgress);
            ClientSession session = queryRunner.getSession();

            // update catalog and schema if present
            if (query.getSetCatalog().isPresent() || query.getSetSchema().isPresent()) {
                session = ClientSession.builder(session)
                        .withCatalog(query.getSetCatalog().orElse(session.getCatalog()))
                        .withSchema(query.getSetSchema().orElse(session.getSchema()))
                        .build();
            }

            // update transaction ID if necessary
            if (query.isClearTransactionId()) {
                session = stripTransactionId(session);
            }

            ClientSession.Builder builder = ClientSession.builder(session);

            if (query.getStartedTransactionId() != null) {
                builder = builder.withTransactionId(query.getStartedTransactionId());
            }

            // update path if present
            if (query.getSetPath().isPresent()) {
                builder = builder.withPath(query.getSetPath().get());
            }

            // update session properties if present
            if (!query.getSetSessionProperties().isEmpty() || !query.getResetSessionProperties().isEmpty()) {
                Map<String, String> sessionProperties = new HashMap<>(session.getProperties());
                sessionProperties.putAll(query.getSetSessionProperties());
                sessionProperties.keySet().removeAll(query.getResetSessionProperties());
                builder = builder.withProperties(sessionProperties);
            }

            // update session roles
            if (!query.getSetRoles().isEmpty()) {
                Map<String, ClientSelectedRole> roles = new HashMap<>(session.getRoles());
                roles.putAll(query.getSetRoles());
                builder = builder.withRoles(roles);
            }

            // update prepared statements if present
            if (!query.getAddedPreparedStatements().isEmpty() || !query.getDeallocatedPreparedStatements().isEmpty()) {
                Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                preparedStatements.putAll(query.getAddedPreparedStatements());
                preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements());
                builder = builder.withPreparedStatements(preparedStatements);
            }

            session = builder.build();
            queryRunner.setSession(session);

            if (query.getSetCatalog().isPresent() || query.getSetSchema().isPresent()) {
                schemaChanged.run();
            }

            return success;
        }
        catch (RuntimeException e) {
            System.err.println("Error running command: " + e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace(System.err);
            }
            return false;
        }
    }

    private static Path getHistoryFile()
    {
        String path = System.getenv("PRESTO_HISTORY_FILE");
        if (!isNullOrEmpty(path)) {
            return Paths.get(path);
        }
        return Paths.get(nullToEmpty(USER_HOME.value()), ".presto_history");
    }

    private static void initializeLogging(String logLevelsFile)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;

        try {
            LoggingConfiguration config = new LoggingConfiguration();

            if (logLevelsFile == null) {
                System.setOut(new PrintStream(nullOutputStream()));
                System.setErr(new PrintStream(nullOutputStream()));

                config.setConsoleEnabled(false);
            }
            else {
                config.setLevelsFile(logLevelsFile);
            }

            Logging logging = Logging.initialize();
            logging.configure(config);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    public boolean runQuery(QueryRunner queryRunner,
                                  String sql,
                                  ClientOptions.OutputFormat outputFormat,
                                  Runnable schemaChanged,
                                  boolean usePager,
                                  boolean showProgress,
                                  Terminal terminal,
                                  PrintStream out,
                                  PrintStream errorChannel)
    {
        return process(queryRunner, sql, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
    }
}
