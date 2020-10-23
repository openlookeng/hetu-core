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
package io.prestosql.queryeditorui.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.units.DataSize;
import io.prestosql.client.Column;
import io.prestosql.client.ErrorLocation;
import io.prestosql.client.FailureInfo;
import io.prestosql.client.QueryData;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.execution.QueryStats;
import io.prestosql.queryeditorui.execution.ExecutionClient.ExecutionFailureException;
import io.prestosql.queryeditorui.execution.QueryClient.QueryTimeOutException;
import io.prestosql.queryeditorui.execution.QueryInfoClient.BasicQueryInfo;
import io.prestosql.queryeditorui.output.builders.FileTooLargeException;
import io.prestosql.queryeditorui.output.builders.JobOutputBuilder;
import io.prestosql.queryeditorui.output.builders.OutputBuilderFactory;
import io.prestosql.queryeditorui.output.persistors.Persistor;
import io.prestosql.queryeditorui.output.persistors.PersistorFactory;
import io.prestosql.queryeditorui.protocol.Job;
import io.prestosql.queryeditorui.protocol.JobSessionContext;
import io.prestosql.queryeditorui.protocol.JobState;
import io.prestosql.queryeditorui.protocol.Table;
import io.prestosql.sql.parser.ParsingException;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class Execution
        implements Callable<Job>
{
    private final Job job;
    private final QueryRunner queryRunner;
    private final QueryInfoClient queryInfoClient;
    private final QueryExecutionAuthorizer authorizer;
    private final Duration timeout;
    private final OutputBuilderFactory outputBuilderFactory;
    private final PersistorFactory persistorFactory;
    private final RateLimiter updateLimiter = RateLimiter.create(2.0);
    private final int maxRowsPreviewOutput = 1_000;
    private boolean isCancelled;
    private URI requestUri;

    public Execution(Job job, QueryRunner queryRunner, QueryInfoClient queryInfoClient,
            QueryExecutionAuthorizer authorizer, Duration timeout, OutputBuilderFactory outputBuilderFactory,
            PersistorFactory persistorFactory, URI requestUri)
    {
        this.job = job;
        this.queryRunner = queryRunner;
        this.queryInfoClient = queryInfoClient;
        this.authorizer = authorizer;
        this.timeout = timeout;
        this.outputBuilderFactory = outputBuilderFactory;
        this.persistorFactory = persistorFactory;
        this.requestUri = requestUri;
    }

    public void cancel()
    {
        isCancelled = true;
    }

    @Override
    public Job call() throws Exception
    {
        return doExecute();
    }

    private Job doExecute()
            throws ExecutionFailureException
    {
        final String userQuery = QUERY_SPLITTER.splitToList(getJob().getQuery()).get(0);
        final JobOutputBuilder outputBuilder;
        job.setQueryStats(createNoOpQueryStats());

        try {
            outputBuilder = outputBuilderFactory.forJob(job);
        }
        catch (IOException e) {
            throw new ExecutionFailureException(job, "Could not create output builder for job", e);
        }
        catch (InvalidQueryException e) {
            throw new ExecutionFailureException(job, e.getMessage(), e);
        }

        final Persistor persistor = persistorFactory.getPersistor(job, job.getOutput());
        final String query = job.getOutput().processQuery(userQuery);

        if (!persistor.canPersist(authorizer)) {
            throw new ExecutionFailureException(job, "Not authorized to create tables", null);
        }

        final Set<Table> tables = new HashSet<>();

        try {
            tables.addAll(authorizer.tablesUsedByQuery(query));
        }
        catch (ParsingException e) {
            job.setError(new QueryError(e.getMessage(), null, -1, null, Optional.empty(),
                    null, new ErrorLocation(e.getLineNumber(), e.getColumnNumber()), null));

            throw new ExecutionFailureException(job, "Invalid query, could not parse", e);
        }

        if (!authorizer.isAuthorizedRead(tables)) {
            job.setQueryStats(createNoOpQueryStats());

            throw new ExecutionFailureException(job, "Cannot access tables", null);
        }

        JobSessionContext jobSessionContext = JobSessionContext.buildFromClient(queryRunner.getSession());
        job.setSessionContext(jobSessionContext);

        QueryClient queryClient = new QueryClient(queryRunner, timeout, query);
        try {
            queryClient.executeWith((client) -> {
                if (client == null) {
                    return null;
                }

                QueryStatusInfo statusInfo = client.currentStatusInfo();
                QueryData data = client.currentData();
                List<Column> resultColumns = null;
                JobState jobState = null;
                QueryError queryError = null;
                QueryStats queryStats = null;

                if (isCancelled) {
                    throw new ExecutionFailureException(job,
                            "Query was cancelled",
                            null);
                }

                if (statusInfo.getError() != null) {
                    queryError = statusInfo.getError();
                    jobState = JobState.FAILED;
                }

                if ((statusInfo.getInfoUri() != null) && (jobState != JobState.FAILED)) {
                    BasicQueryInfo queryInfo = queryInfoClient.from(statusInfo.getInfoUri(), statusInfo.getId());

                    if (queryInfo != null) {
                        queryStats = queryInfo.getQueryStats();
                    }
                }

                if (statusInfo.getInfoUri() != null && job.getInfoUri() == null) {
                    URI infoUri = statusInfo.getInfoUri();
                    String path = infoUri.getPath();
                    path = path.substring(path.indexOf("query.html"));
                    infoUri = URI.create(path + "?" + infoUri.getQuery());
                    job.setInfoUri(infoUri);
                }

                if (statusInfo.getStats() != null) {
                    jobState = JobState.fromStatementState(statusInfo.getStats().getState());
                }

                try {
                    if (statusInfo.getColumns() != null) {
                        resultColumns = statusInfo.getColumns();
                        outputBuilder.addColumns(resultColumns);
                    }

                    if (data.getData() != null) {
                        List<List<Object>> resultsData = ImmutableList.copyOf(data.getData());

                        for (List<Object> row : resultsData) {
                            outputBuilder.addRow(row);
                        }
                    }
                }
                catch (FileTooLargeException e) {
                    throw new ExecutionFailureException(job,
                            "Output file exceeded maximum configured filesize",
                            e);
                }

                rlUpdateJobInfo(tables, resultColumns, queryStats, jobState, queryError);

                return null;
            });
        }
        catch (QueryTimeOutException e) {
            throw new ExecutionFailureException(job,
                    format("Query exceeded maximum execution time of %s minutes", Duration.millis(e.getElapsedMs()).getStandardMinutes()),
                    e);
        }

        QueryStatusInfo finalResults = queryClient.finalResults();
        if (finalResults != null && finalResults.getInfoUri() != null) {
            BasicQueryInfo queryInfo = queryInfoClient.from(finalResults.getInfoUri(), finalResults.getId());

            if (queryInfo != null) {
                updateJobInfo(
                        null,
                        null,
                        queryInfo.getQueryStats(),
                        JobState.fromStatementState(finalResults.getStats().getState()),
                        finalResults.getError());
            }
        }

        if (job.getState() != JobState.FAILED) {
            URI location = persistor.persist(outputBuilder, job);
            if (location != null) {
                job.getOutput().setLocation(location);
            }
        }
        else {
            throw new ExecutionFailureException(job, null, null);
        }

        return getJob();
    }

    public Job getJob()
    {
        return job;
    }

    static final Splitter QUERY_SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();

    /**
     * Rate Limited updateJobInfo
     */
    protected void rlUpdateJobInfo(
            Set<Table> usedTables,
            List<Column> columns,
            QueryStats queryStats,
            JobState state,
            QueryError error)
    {
        if (updateLimiter.tryAcquire(1)) {
            updateJobInfo(usedTables, columns, queryStats, state, error);
        }
        else {
            updateJobInfo(usedTables, columns, queryStats, state, error);
        }
    }

    protected void updateJobInfo(
            Set<Table> usedTables,
            List<Column> columns,
            QueryStats queryStats,
            JobState state,
            QueryError error)
    {
        if ((usedTables != null) && (usedTables.size() > 0)) {
            job.getTablesUsed().addAll(usedTables);
        }

        if ((columns != null) && (columns.size() > 0)) {
            job.setColumns(columns);
        }

        if (queryStats != null) {
            job.setQueryStats(queryStats);
        }

        if ((state != null) && (job.getState() != JobState.FINISHED) && (job.getState() != JobState.FAILED)) {
            job.setState(state);
        }

        if (error != null) {
            FailureInfo failureInfo = new FailureInfo(
                    error.getFailureInfo().getType(),
                    error.getFailureInfo().getMessage(),
                    null,
                    Collections.<FailureInfo>emptyList(),
                    Collections.<String>emptyList(),
                    error.getFailureInfo().getErrorLocation());

            QueryError queryError = new QueryError(
                    error.getMessage(),
                    error.getSqlState(),
                    error.getErrorCode(),
                    error.getErrorName(),
                    error.getSemanticErrorName(),
                    error.getErrorType(),
                    error.getErrorLocation(),
                    failureInfo);

            job.setError(queryError);
        }
    }

    public static QueryStats createNoOpQueryStats()
    {
        DateTime now = DateTime.now();
        io.airlift.units.Duration zeroDuration = new io.airlift.units.Duration(0, TimeUnit.SECONDS);
        DataSize zeroData = new DataSize(0, DataSize.Unit.BYTE);

        return new QueryStats(
                now,
                null,
                now,
                now,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0.0,
                zeroData,
                zeroData,
                zeroData,
                zeroData,
                zeroData,
                zeroData,
                zeroData,
                zeroData,
                zeroData,
                false,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                false,
                ImmutableSet.of(),
                zeroData,
                0,
                zeroData,
                0,
                zeroData,
                0,
                zeroData,
                0,
                zeroData,
                0,
                zeroData,
                ImmutableList.of(),
                ImmutableList.of());
    }
}
