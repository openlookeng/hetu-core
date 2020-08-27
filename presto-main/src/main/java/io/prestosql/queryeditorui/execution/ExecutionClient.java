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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.prestosql.client.ClientSelectedRole;
import io.prestosql.client.ClientSession;
import io.prestosql.client.QueryError;
import io.prestosql.client.StatementClient;
import io.prestosql.queryeditorui.execution.QueryRunner.QueryRunnerFactory;
import io.prestosql.queryeditorui.output.PersistentJobOutputFactory;
import io.prestosql.queryeditorui.output.builders.OutputBuilderFactory;
import io.prestosql.queryeditorui.output.persistors.PersistorFactory;
import io.prestosql.queryeditorui.protocol.ExecutionRequest;
import io.prestosql.queryeditorui.protocol.Job;
import io.prestosql.queryeditorui.protocol.JobSessionContext;
import io.prestosql.queryeditorui.protocol.JobState;
import io.prestosql.queryeditorui.store.history.JobHistoryStore;
import io.prestosql.queryeditorui.store.jobs.jobs.ActiveJobsStore;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static io.prestosql.queryeditorui.execution.Execution.QUERY_SPLITTER;

public class ExecutionClient
{
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
            Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                    .setNameFormat("execution-client-%d").setDaemon(true).build()));

    private final PersistentJobOutputFactory persistentJobOutputFactory;
    private final QueryInfoClient queryInfoClient;
    private final QueryRunnerFactory queryRunnerFactory;
    private final ActiveJobsStore activeJobsStore;
    private final OutputBuilderFactory outputBuilderFactory;
    private final PersistorFactory persistorFactory;
    private final Map<UUID, Execution> executionMap = new ConcurrentHashMap<>();
    private final JobHistoryStore historyStore;

    @Inject
    public ExecutionClient(JobHistoryStore historyStore,
                           PersistentJobOutputFactory persistentJobOutputFactory,
                           QueryInfoClient queryInfoClient,
                           QueryRunnerFactory queryRunnerFactory,
                           ActiveJobsStore activeJobsStore,
                           OutputBuilderFactory outputBuilderFactory,
                           PersistorFactory persistorFactory)
    {
        this.historyStore = historyStore;
        this.persistentJobOutputFactory = persistentJobOutputFactory;
        this.queryInfoClient = queryInfoClient;
        this.queryRunnerFactory = queryRunnerFactory;
        this.activeJobsStore = activeJobsStore;
        this.outputBuilderFactory = outputBuilderFactory;
        this.persistorFactory = persistorFactory;
    }

    public List<UUID> runQuery(final ExecutionRequest request,
                         final String user,
                         final Duration timeout,
                         HttpServletRequest servletRequest)
    {
        String query = request.getQuery();
        JobSessionContext sessionContext = request.getSessionContext();
        Map<String, String> properties = sessionContext != null && sessionContext.getProperties() != null ?
                sessionContext.getProperties() : ImmutableMap.of();
        QueryRunner queryRunner = queryRunnerFactory.create(user, request.getDefaultConnector(), request.getDefaultSchema(), properties);
        QueryExecutionAuthorizer authorizer = new QueryExecutionAuthorizer(user, request.getDefaultConnector(), request.getDefaultSchema());

        // When multiple statements are submitted together, split them and execute in sequence.
        List<String> subStatements = QUERY_SPLITTER.splitToList(query);
        BlockingQueue<Job> jobs = new ArrayBlockingQueue<>(subStatements.size());
        ImmutableList.Builder<UUID> results = ImmutableList.builder();
        URI requestURI = URI.create(servletRequest.getRequestURL().toString());

        for (String statement : subStatements) {
            final UUID uuid = UUID.randomUUID();
            Job job = new Job(user,
                    statement,
                    uuid,
                    persistentJobOutputFactory.create(null, uuid),
                    null,
                    JobState.QUEUED,
                    Collections.emptyList(),
                    null,
                    null,
                    null);
            results.add(job.getUuid());
            jobs.offer(job);
        }
        scheduleExecution(timeout, queryRunner, authorizer, jobs, requestURI);
        return results.build();
    }

    private UUID scheduleExecution(Duration timeout,
                                   QueryRunner queryRunner,
                                   QueryExecutionAuthorizer authorizer,
                                   BlockingQueue<Job> jobs,
                                   URI requestUri)
    {
        final Job job;
        try {
            job = jobs.take();
        }
        catch (InterruptedException e) {
            return null;
        }

        final Execution execution = new Execution(job,
                queryRunner,
                queryInfoClient,
                authorizer,
                timeout,
                outputBuilderFactory,
                persistorFactory,
                requestUri);

        executionMap.put(job.getUuid(), execution);
        activeJobsStore.jobStarted(job);

        ListenableFuture<Job> result = executor.submit(execution);
        Futures.addCallback(result, new FutureCallback<Job>()
        {
            @Override
            public void onSuccess(@Nullable Job result)
            {
                if (result != null) {
                    result.setState(JobState.FINISHED);
                }
                //Add Active Job
                if (jobs.peek() != null) {
                    QueryRunner nextQueryRunner = getNextQueryRunner();
                    scheduleExecution(timeout, nextQueryRunner, authorizer, jobs, requestUri);
                }
                jobFinished(result);
            }

            //Re-Use session level fields among multi statement queries.
            private QueryRunner getNextQueryRunner()
            {
                StatementClient client = queryRunner.getCurrentClient();
                ClientSession session = queryRunner.getSession();
                ClientSession.Builder builder = ClientSession.builder(session)
                        .withoutTransactionId();
                if (client.getSetCatalog().isPresent()) {
                    builder.withCatalog(client.getSetCatalog().get());
                }
                if (client.getSetSchema().isPresent()) {
                    builder.withSchema(client.getSetSchema().get());
                }
                if (client.getStartedTransactionId() != null) {
                    builder = builder.withTransactionId(client.getStartedTransactionId());
                }
                if (client.getSetPath().isPresent()) {
                    builder = builder.withPath(client.getSetPath().get());
                }
                if (!client.getSetSessionProperties().isEmpty() || !client.getResetSessionProperties().isEmpty()) {
                    Map<String, String> sessionProperties = new HashMap<>(session.getProperties());
                    sessionProperties.putAll(client.getSetSessionProperties());
                    sessionProperties.keySet().removeAll(client.getResetSessionProperties());
                    builder = builder.withProperties(sessionProperties);
                }
                if (!client.getSetRoles().isEmpty()) {
                    Map<String, ClientSelectedRole> roles = new HashMap<>(session.getRoles());
                    roles.putAll(client.getSetRoles());
                    builder = builder.withRoles(roles);
                }
                if (!client.getAddedPreparedStatements().isEmpty() || !client.getDeallocatedPreparedStatements().isEmpty()) {
                    Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                    preparedStatements.putAll(client.getAddedPreparedStatements());
                    preparedStatements.keySet().removeAll(client.getDeallocatedPreparedStatements());
                    builder = builder.withPreparedStatements(preparedStatements);
                }
                return queryRunnerFactory.create(builder.build());
            }

            @Override
            public void onFailure(@NotNull Throwable t)
            {
                job.setState(JobState.FAILED);
                if (job.getError() == null) {
                    job.setError(new QueryError(t.getMessage(), null, -1, null,
                            null, null, null, null));
                }

                jobFinished(job);
            }
        }, MoreExecutors.directExecutor());

        return job.getUuid();
    }

    protected void jobFinished(Job job)
    {
        job.setQueryFinished(new DateTime());
        historyStore.addRun(job);
        activeJobsStore.jobFinished(job);

        executionMap.remove(job.getUuid());
    }

    public boolean cancelQuery(
            String user,
            UUID uuid)
    {
        Execution execution = executionMap.get(uuid);

        if ((execution != null) && (execution.getJob().getUser().equals(user))) {
            execution.cancel();
            return true;
        }
        else {
            return false;
        }
    }

    public static class ExecutionFailureException
            extends RuntimeException
    {
        private final Job job;

        public ExecutionFailureException(Job job, String message, Throwable cause)
        {
            super(message, cause);
            this.job = job;
        }

        public Job getJob()
        {
            return job;
        }
    }
}
