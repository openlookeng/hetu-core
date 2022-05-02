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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.client.ClientSelectedRole;
import io.prestosql.client.Column;
import io.prestosql.client.ErrorLocation;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.client.StatementClient;
import io.prestosql.client.Warning;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;

public class Query
        implements Closeable
{
    private static final Signal SIGINT = new Signal("INT");

    private static final Logger LOG = Logger.get(Query.class);

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;
    private final boolean debug;
    private String cubeInitQueryResult;

    public Query(StatementClient client, boolean debug)
    {
        this.client = requireNonNull(client, "client is null");
        this.debug = debug;
    }

    public Optional<String> getSetCatalog()
    {
        return client.getSetCatalog();
    }

    public Optional<String> getSetSchema()
    {
        return client.getSetSchema();
    }

    public Optional<String> getSetPath()
    {
        return client.getSetPath();
    }

    public Map<String, String> getSetSessionProperties()
    {
        return client.getSetSessionProperties();
    }

    public Set<String> getResetSessionProperties()
    {
        return client.getResetSessionProperties();
    }

    public Map<String, ClientSelectedRole> getSetRoles()
    {
        return client.getSetRoles();
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return client.getAddedPreparedStatements();
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return client.getDeallocatedPreparedStatements();
    }

    public String getStartedTransactionId()
    {
        return client.getStartedTransactionId();
    }

    public boolean isClearTransactionId()
    {
        return client.isClearTransactionId();
    }

    public boolean renderOutput(Terminal terminal, PrintStream out, PrintStream errorChannel, ClientOptions.OutputFormat outputFormat, boolean usePager, boolean showProgress)
    {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = Signal.handle(SIGINT, signal -> {
            if (ignoreUserInterrupt.get() || client.isClientAborted()) {
                return;
            }
            client.close();
            clientThread.interrupt();
        });
        try {
            return renderQueryOutput(terminal, out, errorChannel, outputFormat, usePager, showProgress);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private boolean renderQueryOutput(Terminal terminal, PrintStream out, PrintStream errorChannel, ClientOptions.OutputFormat outputFormat, boolean usePager, boolean showProgress)
    {
        StatusPrinter statusPrinter = null;
        WarningsPrinter warningsPrinter = new PrintStreamWarningsPrinter(errorChannel);
        if (showProgress) {
            statusPrinter = new StatusPrinter(client, errorChannel, debug);
            statusPrinter.printInitialStatusUpdates(terminal);
        }
        else {
            processInitialStatusUpdates(warningsPrinter);
        }

        // if running or finished
        if (client.isRunning() || (client.isFinished() && client.finalStatusInfo().getError() == null)) {
            QueryStatusInfo results = client.isRunning() ? client.currentStatusInfo() : client.finalStatusInfo();
            if (results.getUpdateType() != null) {
                renderUpdate(errorChannel, results);
            }
            else if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return false;
            }
            else {
                renderResults(out, outputFormat, usePager, results.getColumns());
            }
        }

        checkState(!client.isRunning());

        warningsPrinter.print(client.finalStatusInfo().getWarnings(), true, true);

        if (showProgress) {
            statusPrinter.printFinalInfo();
        }

        if (client.isClientAborted()) {
            errorChannel.println("Query aborted by user");
            return false;
        }
        if (client.isClientError()) {
            errorChannel.println("Query is gone (server restarted?)");
            return false;
        }

        verify(client.isFinished());
        if (client.finalStatusInfo().getError() != null) {
            renderFailure(errorChannel);
            return false;
        }

        return true;
    }

    private void processInitialStatusUpdates(WarningsPrinter warningsPrinter)
    {
        while (client.isRunning() && (client.currentData().getData() == null)) {
            warningsPrinter.print(client.currentStatusInfo().getWarnings(), true, false);
            try {
                client.advance();
            }
            catch (RuntimeException e) {
                LOG.debug(e, "error printing status");
            }
        }
        List<Warning> warnings;
        if (client.isRunning()) {
            warnings = client.currentStatusInfo().getWarnings();
        }
        else {
            warnings = client.finalStatusInfo().getWarnings();
        }
        warningsPrinter.print(warnings, false, true);
    }

    private void renderUpdate(PrintStream out, QueryStatusInfo results)
    {
        String status = results.getUpdateType();
        if (results.getUpdateCount() != null) {
            long count = results.getUpdateCount();
            status += format(": %s row%s", count, (count != 1) ? "s" : "");
        }
        out.println(status);
        discardResults();
    }

    private void discardResults()
    {
        try (OutputHandler handler = new OutputHandler(new NullPrinter())) {
            handler.processRows(client);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void renderResults(PrintStream out, ClientOptions.OutputFormat outputFormat, boolean interactive, List<Column> columns)
    {
        try {
            doRenderResults(out, outputFormat, interactive, columns);
        }
        catch (QueryAbortedException e) {
            System.out.println("(query aborted by user)");
            client.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void doRenderResults(PrintStream out, ClientOptions.OutputFormat format, boolean interactive, List<Column> columns)
            throws IOException
    {
        if (interactive) {
            pageOutput(format, columns);
        }
        else {
            sendOutput(out, format, columns);
        }
    }

    private void pageOutput(ClientOptions.OutputFormat format, List<Column> columns)
            throws IOException
    {
        try (Pager pager = Pager.create();
                ThreadInterruptor clientThread = new ThreadInterruptor();
                Writer writer = createWriter(pager);
                OutputHandler handler = createOutputHandler(format, writer, columns)) {
            if (!pager.isNullPager()) {
                // ignore the user pressing ctrl-C while in the pager
                ignoreUserInterrupt.set(true);
                pager.getFinishFuture().thenRun(() -> {
                    ignoreUserInterrupt.set(false);
                    client.close();
                    clientThread.interrupt();
                });
            }
            handler.processRows(client);
        }
        catch (RuntimeException | IOException e) {
            if (client.isClientAborted() && !(e instanceof QueryAbortedException)) {
                throw new QueryAbortedException(e);
            }
            throw e;
        }
    }

    private void sendOutput(PrintStream out, ClientOptions.OutputFormat format, List<Column> fieldNames)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(format, createWriter(out), fieldNames)) {
            handler.processRows(client);
        }
    }

    private static OutputHandler createOutputHandler(ClientOptions.OutputFormat format, Writer writer, List<Column> columns)
    {
        return new OutputHandler(createOutputPrinter(format, writer, columns));
    }

    private static OutputPrinter createOutputPrinter(ClientOptions.OutputFormat format, Writer writer, List<Column> columns)
    {
        List<String> fieldNames = columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        switch (format) {
            case ALIGNED:
                return new AlignedTablePrinter(columns, writer);
            case VERTICAL:
                return new VerticalRecordPrinter(fieldNames, writer);
            case CSV:
                return new CsvPrinter(fieldNames, writer, CsvPrinter.CsvOutputFormat.NO_HEADER);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, CsvPrinter.CsvOutputFormat.STANDARD);
            case CSV_UNQUOTED:
                return new CsvPrinter(fieldNames, writer, CsvPrinter.CsvOutputFormat.NO_HEADER_AND_QUOTES);
            case CSV_HEADER_UNQUOTED:
                return new CsvPrinter(fieldNames, writer, CsvPrinter.CsvOutputFormat.NO_QUOTES);
            case TSV:
                return new TsvPrinter(fieldNames, writer, false);
            case TSV_HEADER:
                return new TsvPrinter(fieldNames, writer, true);
            case JSON:
                return new JsonPrinter(fieldNames, writer);
            case NULL:
                return new NullPrinter();
        }
        throw new RuntimeException(format + " not supported");
    }

    private static Writer createWriter(OutputStream out)
    {
        return new OutputStreamWriter(out, UTF_8);
    }

    @Override
    public void close()
    {
        client.close();
    }

    public String getcubeInitQueryResult()
    {
        return cubeInitQueryResult;
    }

    public void renderFailure(PrintStream out)
    {
        QueryStatusInfo results = client.finalStatusInfo();
        QueryError error = results.getError();
        checkState(error != null);

        out.printf("Query %s failed: %s%n", results.getId(), error.getMessage());
        if (debug && (error.getFailureInfo() != null)) {
            error.getFailureInfo().toException().printStackTrace(out);
        }
        if (error.getErrorLocation() != null) {
            renderErrorLocation(client.getQuery(), error.getErrorLocation(), out);
        }
        out.println();
    }

    static void renderErrorLocation(String query, ErrorLocation location, PrintStream out)
    {
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(location.getLineNumber() - 1);
        String good = errorLine.substring(0, location.getColumnNumber() - 1);
        String bad = errorLine.substring(location.getColumnNumber() - 1);

        if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (ConsolePrinter.REAL_TERMINAL) {
            AttributedStringBuilder builder = new AttributedStringBuilder();

            builder.style(DEFAULT.foreground(CYAN));
            for (int i = 1; i < location.getLineNumber(); i++) {
                builder.append(lines.get(i - 1)).append("\n");
            }
            builder.append(good);

            builder.style(DEFAULT.foreground(RED));
            builder.append(bad).append("\n");
            for (int i = location.getLineNumber(); i < lines.size(); i++) {
                builder.append(lines.get(i)).append("\n");
            }

            builder.style(DEFAULT);
            out.print(builder.toAnsi());
        }
        else {
            String prefix = format("LINE %s: ", location.getLineNumber());
            String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
            out.println(prefix + errorLine);
            out.println(padding + "^");
        }
    }

    private static class PrintStreamWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private final PrintStream printStream;

        PrintStreamWarningsPrinter(PrintStream printStream)
        {
            super(OptionalInt.empty());
            this.printStream = requireNonNull(printStream, "printStream is null");
        }

        @Override
        protected void print(List<String> warnings)
        {
            warnings.stream()
                    .forEach(printStream::println);
        }

        @Override
        protected void printSeparator()
        {
            printStream.println();
        }
    }
}
