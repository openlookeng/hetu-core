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
package io.prestosql.server.protocol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.Session;
import io.prestosql.client.ClientTypeSignature;
import io.prestosql.client.ClientTypeSignatureParameter;
import io.prestosql.client.Column;
import io.prestosql.client.DataCenterQueryResults;
import io.prestosql.client.FailureInfo;
import io.prestosql.client.NamedClientTypeSignature;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryResults;
import io.prestosql.client.RowFieldName;
import io.prestosql.client.SnapshotStats;
import io.prestosql.client.StageStats;
import io.prestosql.client.StatementStats;
import io.prestosql.client.Warning;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.TaskInfo;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.PipelineStats;
import io.prestosql.operator.TaskLocation;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.snapshot.RestoreResult;
import io.prestosql.snapshot.SnapshotInfo;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.WarningCode;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.transaction.TransactionId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.prestosql.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.prestosql.SystemSessionProperties.isSnapshotEnabled;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.RESCHEDULING;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.util.Failures.toFailure;
import static io.prestosql.util.MoreLists.mappedCopy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class Query
{
    private static final Logger log = Logger.get(Query.class);

    private final QueryManager queryManager;
    private final QueryId queryId;
    private final Session session;
    private final String slug;

    @GuardedBy("this")
    private final ExchangeClient exchangeClient;

    private final Executor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final PagesSerde serde;

    @GuardedBy("this")
    private OptionalLong nextToken = OptionalLong.of(0);

    @GuardedBy("this")
    private QueryResults lastResult;

    @GuardedBy("this")
    private DataCenterQueryResults lastDCQueryResult;

    @GuardedBy("this")
    private long lastToken = -1;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

    @GuardedBy("this")
    private Optional<String> setCatalog = Optional.empty();

    @GuardedBy("this")
    private Optional<String> setSchema = Optional.empty();

    @GuardedBy("this")
    private Optional<String> setPath = Optional.empty();

    @GuardedBy("this")
    private Map<String, String> setSessionProperties = ImmutableMap.of();

    @GuardedBy("this")
    private Set<String> resetSessionProperties = ImmutableSet.of();

    @GuardedBy("this")
    private Map<String, SelectedRole> setRoles = ImmutableMap.of();

    @GuardedBy("this")
    private Map<String, String> addedPreparedStatements = ImmutableMap.of();

    @GuardedBy("this")
    private Set<String> deallocatedPreparedStatements = ImmutableSet.of();

    @GuardedBy("this")
    private Optional<TransactionId> startedTransactionId = Optional.empty();

    @GuardedBy("this")
    private boolean clearTransactionId;

    @GuardedBy("this")
    private Long updateCount;

    public static Query create(
            Session session,
            String slug,
            QueryManager queryManager,
            ExchangeClient exchangeClient,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        Query result = new Query(session, slug, queryManager, exchangeClient, dataProcessorExecutor, timeoutExecutor, blockEncodingSerde);

        result.queryManager.addOutputInfoListener(result.getQueryId(), result::setQueryOutputInfo);
        result.queryManager.addStateChangeListener(result.getQueryId(), result::updateQueryState);

        result.queryManager.addStateChangeListener(result.getQueryId(), state -> {
            if (state.isDone()) {
                QueryInfo queryInfo = queryManager.getFullQueryInfo(result.getQueryId());
                result.closeExchangeClientIfNecessary(queryInfo);
            }
        });

        return result;
    }

    private Query(
            Session session,
            String slug,
            QueryManager queryManager,
            ExchangeClient exchangeClient,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        requireNonNull(session, "session is null");
        requireNonNull(slug, "slug is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(exchangeClient, "exchangeClient is null");
        requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        requireNonNull(blockEncodingSerde, "serde is null");

        this.queryManager = queryManager;

        this.queryId = session.getQueryId();
        this.session = session;
        this.slug = slug;
        this.exchangeClient = exchangeClient;
        this.resultsProcessorExecutor = resultsProcessorExecutor;
        this.timeoutExecutor = timeoutExecutor;

        serde = new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)).createPagesSerde();
    }

    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    public synchronized void dispose()
    {
        exchangeClient.close();
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public boolean isSlugValid(String slug)
    {
        return this.slug.equals(slug);
    }

    public synchronized Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    public synchronized Optional<String> getSetSchema()
    {
        return setSchema;
    }

    public synchronized Optional<String> getSetPath()
    {
        return setPath;
    }

    public synchronized Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public synchronized Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public synchronized Map<String, SelectedRole> getSetRoles()
    {
        return setRoles;
    }

    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    public synchronized ListenableFuture<QueryResults> waitForResults(long token, UriInfo uriInfo, String scheme, Duration wait, DataSize targetResultSize)
    {
        // before waiting, check if this request has already been processed and cached
        Optional<QueryResults> cachedResult = getCachedResult(token);
        if (cachedResult.isPresent()) {
            return immediateFuture(cachedResult.get());
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResult(token, uriInfo, scheme, targetResultSize), resultsProcessorExecutor);
    }

    public synchronized ListenableFuture<DataCenterQueryResults> waitForResults(long token, Duration wait, DataSize targetResultSize)
    {
        // before waiting, check if this request has already been processed and cached
        Optional<DataCenterQueryResults> cachedResult = getCachedDCQueryResult(token);
        if (cachedResult.isPresent()) {
            return immediateFuture(cachedResult.get());
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResult(token, targetResultSize), resultsProcessorExecutor);
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        // if the exchange client is open, wait for data
        if (!exchangeClient.isClosed()) {
            return exchangeClient.isBlocked();
        }

        // otherwise, wait for the query to finish
        queryManager.recordHeartbeat(queryId);
        try {
            return queryDoneFuture(queryManager.getQueryState(queryId));
        }
        catch (NoSuchElementException e) {
            return immediateFuture(null);
        }
    }

    private synchronized Optional<QueryResults> getCachedResult(long token)
    {
        // is this the first request?
        if (lastResult == null) {
            return Optional.empty();
        }

        // is the a repeated request for the last results?
        if (token == lastToken) {
            // tell query manager we are still interested in the query
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastResult);
        }

        // if this is a result before the lastResult, the data is gone
        if (token < lastToken) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // if this is a request for a result after the end of the stream, return not found
        if (!nextToken.isPresent()) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        // if this is not a request for the next results, return not found
        if (token != nextToken.getAsLong()) {
            // unknown token
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return Optional.empty();
    }

    private synchronized Optional<DataCenterQueryResults> getCachedDCQueryResult(long token)
    {
        // is this the first request?
        if (lastDCQueryResult == null) {
            return Optional.empty();
        }

        // is the a repeated request for the last results?
        if (token == lastToken) {
            // tell query manager we are still interested in the query
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastDCQueryResult);
        }

        // if this is a result before the lastResult, the data is gone
        if (token < lastToken) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // if this is a request for a result after the end of the stream, return not found
        if (!nextToken.isPresent()) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        // if this is not a request for the next results, return not found
        if (token != nextToken.getAsLong()) {
            // unknown token
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return Optional.empty();
    }

    private synchronized QueryResults getNextResult(long token, UriInfo uriInfo, String scheme, DataSize targetResultSize)
    {
        // check if the result for the token have already been created
        Optional<QueryResults> cachedResult = getCachedResult(token);
        if (cachedResult.isPresent()) {
            return cachedResult.get();
        }

        verify(nextToken.isPresent(), "Can not generate next result when next token is not present");
        verify(token == nextToken.getAsLong(), "Expected token to equal next token");
        URI queryHtmlUri = uriInfo.getRequestUriBuilder()
                .scheme(scheme)
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();

        // Remove as many pages as possible from the exchange until just greater than DESIRED_RESULT_BYTES
        // NOTE: it is critical that query results are created for the pages removed from the exchange
        // client while holding the lock because the query may transition to the finished state when the
        // last page is removed.  If another thread observes this state before the response is cached
        // the pages will be lost.
        Iterable<List<Object>> data = null;
        try {
            ImmutableList.Builder<RowIterable> pages = ImmutableList.builder();
            long bytes = 0;
            long rows = 0;
            long targetResultBytes = targetResultSize.toBytes();
            while (bytes < targetResultBytes) {
                // at this point, origin is irrelevant, so we can safely ignore it
                SerializedPage serializedPage = exchangeClient.pollPage(null).getLeft();
                if (serializedPage == null) {
                    break;
                }

                Page page = serde.deserialize(serializedPage);
                bytes += page.getLogicalSizeInBytes();
                rows += page.getPositionCount();
                pages.add(new RowIterable(session.toConnectorSession(), types, page));
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = Iterables.concat(pages.build());
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        // TODO: figure out a better way to do this
        // grab the update count for non-queries
        if ((data != null) && (queryInfo.getUpdateType() != null) && (updateCount == null) &&
                (columns.size() == 1) && (columns.get(0).getType().equals(StandardTypes.BIGINT))) {
            Iterator<List<Object>> iterator = data.iterator();
            if (iterator.hasNext()) {
                Number number = (Number) iterator.next().get(0);
                if (number != null) {
                    updateCount = number.longValue();
                }
            }
        }

        closeExchangeClientIfNecessary(queryInfo);

        // for queries with no output, return a fake result for clients that require it
        if ((queryInfo.getState() == QueryState.FINISHED) && !queryInfo.getOutputStage().isPresent()) {
            columns = ImmutableList.of(createColumn("result", BooleanType.BOOLEAN));
            data = ImmutableSet.of(ImmutableList.of(true));
        }
        else if (queryInfo.isRunningAsync()) {
            columns = ImmutableList.of(createColumn("result", BooleanType.BOOLEAN),
                    createColumn("runningAsync", BooleanType.BOOLEAN));
            data = ImmutableSet.of(ImmutableList.of(true, true));
        }

        // advance next token
        // only return a next if
        // (1) the query is not done AND the query state is not FAILED
        //   OR
        // (2)there is more data to send (due to buffering)
        //  OR
        // (3) Query supports and started running Async
        if ((!queryInfo.isFinalQueryInfo() && queryInfo.getState() != FAILED && !queryInfo.isRunningAsync()) ||
                !exchangeClient.isClosed()) {
            nextToken = OptionalLong.of(token + 1);
        }
        else {
            nextToken = OptionalLong.empty();
            queryManager.checkForQueryPruning(queryId, queryInfo);
        }

        URI nextResultsUri = null;
        if (nextToken.isPresent()) {
            nextResultsUri = createNextResultsUri(scheme, uriInfo, nextToken.getAsLong());
        }

        // update catalog, schema, and path
        setCatalog = queryInfo.getSetCatalog();
        setSchema = queryInfo.getSetSchema();
        setPath = queryInfo.getSetPath();

        // update setSessionProperties
        setSessionProperties = queryInfo.getSetSessionProperties();
        resetSessionProperties = queryInfo.getResetSessionProperties();

        // update setRoles
        setRoles = queryInfo.getSetRoles();

        // update preparedStatements
        addedPreparedStatements = queryInfo.getAddedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.getDeallocatedPreparedStatements();

        // update startedTransactionId
        startedTransactionId = queryInfo.getStartedTransactionId();
        clearTransactionId = queryInfo.isClearTransactionId();

        // first time through, self is null
        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                queryHtmlUri,
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                mappedCopy(queryInfo.getWarnings(), Query::toClientWarning),
                queryInfo.getUpdateType(),
                (queryInfo.isRunningAsync() ? null : updateCount));

        // cache the new result
        lastToken = token;
        lastResult = queryResults;

        return queryResults;
    }

    private synchronized DataCenterQueryResults getNextResult(long token, DataSize targetResultSize)
    {
        // check if the result for the token have already been created
        Optional<DataCenterQueryResults> cachedResult = getCachedDCQueryResult(token);
        if (cachedResult.isPresent()) {
            return cachedResult.get();
        }

        verify(nextToken.isPresent(), "Can not generate next result when next token is not present");
        verify(token == nextToken.getAsLong(), "Expected token to equal next token");
        URI queryHtmlUri = null;
        try {
            queryHtmlUri = new URI("http://localhost");
        }
        catch (URISyntaxException e) {
            log.error("get uri error: %s", e.getMessage());
        }

        // Remove as many pages as possible from the exchange until just greater than DESIRED_RESULT_BYTES
        // NOTE: it is critical that query results are created for the pages removed from the exchange
        // client while holding the lock because the query may transition to the finished state when the
        // last page is removed.  If another thread observes this state before the response is cached
        // the pages will be lost.
        List<SerializedPage> data = null;
        try {
            ImmutableList.Builder<SerializedPage> builder = new ImmutableList.Builder<>();
            long bytes = 0;
            long rows = 0;
            long targetResultBytes = targetResultSize.toBytes();
            while (bytes < targetResultBytes) {
                // at this point, origin is irrelevant, so we can safely ignore it
                SerializedPage serializedPage = exchangeClient.pollPage(null).getLeft();
                if (serializedPage == null) {
                    break;
                }
                builder.add(new SerializedPage(
                        serializedPage.getSlice().getBytes(),
                        serializedPage.getPageCodecMarkers(),
                        serializedPage.getPositionCount(),
                        serializedPage.getUncompressedSizeInBytes()));
                bytes += serializedPage.getUncompressedSizeInBytes();
                rows += serializedPage.getPositionCount();
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = builder.build();
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        closeExchangeClientIfNecessary(queryInfo);

        // for queries with no output, return a fake result for clients that require it
        if ((queryInfo.getState() == QueryState.FINISHED) && !queryInfo.getOutputStage().isPresent()) {
            columns = ImmutableList.of(createColumn("result", BooleanType.BOOLEAN));
            PageBuilder pageBuilder = new PageBuilder(Collections.singletonList(BooleanType.BOOLEAN));
            pageBuilder.declarePosition();
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, true);
            Page page = pageBuilder.build();
            SerializedPage serializedPage = serde.serialize(page);
            data = Collections.singletonList(new SerializedPage(
                    serializedPage.getSlice().getBytes(),
                    serializedPage.getPageCodecMarkers(),
                    serializedPage.getPositionCount(),
                    serializedPage.getUncompressedSizeInBytes()));
        }
        else if (queryInfo.isRunningAsync()) {
            columns = ImmutableList.of(createColumn("result", BooleanType.BOOLEAN),
                    createColumn("runningAsync", BooleanType.BOOLEAN));
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BooleanType.BOOLEAN, BooleanType.BOOLEAN));
            pageBuilder.declarePosition();
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, true);
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, true);
            Page page = pageBuilder.build();
            SerializedPage serializedPage = serde.serialize(page);
            data = Collections.singletonList(new SerializedPage(
                    serializedPage.getSlice().getBytes(),
                    serializedPage.getPageCodecMarkers(),
                    serializedPage.getPositionCount(),
                    serializedPage.getUncompressedSizeInBytes()));
        }

        // advance next token
        // only return a next if
        // (1) the query is not done AND the query state is not FAILED
        //   OR
        // (2)there is more data to send (due to buffering)
        //  OR
        // (3) Query supports and started running Async
        if ((!queryInfo.isFinalQueryInfo() && queryInfo.getState() != FAILED && !queryInfo.isRunningAsync()) ||
                !exchangeClient.isClosed()) {
            nextToken = OptionalLong.of(token + 1);
        }
        else {
            nextToken = OptionalLong.empty();
        }

        URI nextResultsUri = null;
        if (nextToken.isPresent()) {
            try {
                nextResultsUri = new URI(Long.toString(nextToken.getAsLong()));
            }
            catch (URISyntaxException e) {
                log.error("get uri error: %s", e.getMessage());
            }
        }

        // update catalog, schema, and path
        setCatalog = queryInfo.getSetCatalog();
        setSchema = queryInfo.getSetSchema();
        setPath = queryInfo.getSetPath();

        // update setSessionProperties
        setSessionProperties = queryInfo.getSetSessionProperties();
        resetSessionProperties = queryInfo.getResetSessionProperties();

        // update setRoles
        setRoles = queryInfo.getSetRoles();

        // update preparedStatements
        addedPreparedStatements = queryInfo.getAddedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.getDeallocatedPreparedStatements();

        // update startedTransactionId
        startedTransactionId = queryInfo.getStartedTransactionId();
        clearTransactionId = queryInfo.isClearTransactionId();

        // first time through, self is null
        DataCenterQueryResults queryResults = new DataCenterQueryResults(
                queryId.toString(),
                queryHtmlUri,
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                mappedCopy(queryInfo.getWarnings(), Query::toClientWarning),
                queryInfo.getUpdateType(),
                true);

        // cache the new result
        lastToken = token;
        lastDCQueryResult = queryResults;

        return queryResults;
    }

    private synchronized void closeExchangeClientIfNecessary(QueryInfo queryInfo)
    {
        // Close the exchange client if the query has failed, or if the query
        // is done and it does not have an output stage. The latter happens
        // for data definition executions, as those do not have output.
        //or query can run in async, after planning
        if ((queryInfo.getState() == FAILED) ||
                (queryInfo.getState().isDone() && !queryInfo.getOutputStage().isPresent()) ||
                queryInfo.isRunningAsync()) {
            exchangeClient.close();
        }
    }

    private synchronized void updateQueryState(QueryState newState)
    {
        if (newState == RESCHEDULING) {
            // Snapshot: remote task will be rescheduled. Need to reset exchange client so it can be connected to new task.
            exchangeClient.resetForResume();
        }
    }

    private synchronized void setQueryOutputInfo(QueryExecution.QueryOutputInfo outputInfo)
    {
        // if first callback, set column names
        if (columns == null) {
            List<String> columnNames = outputInfo.getColumnNames();
            List<Type> columnTypes = outputInfo.getColumnTypes();
            checkArgument(columnNames.size() == columnTypes.size(), "Column names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                list.add(createColumn(columnNames.get(i), columnTypes.get(i)));
            }
            columns = list.build();
            types = outputInfo.getColumnTypes();
        }

        for (TaskLocation outputLocation : outputInfo.getBufferLocations()) {
            exchangeClient.addLocation(outputLocation);
        }
        if (outputInfo.isNoMoreBufferLocations()) {
            exchangeClient.noMoreLocations();
        }
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture, directExecutor());
    }

    private synchronized URI createNextResultsUri(String scheme, UriInfo uriInfo, long nextToken)
    {
        return uriInfo.getBaseUriBuilder()
                .scheme(scheme)
                .replacePath("/v1/statement/executing")
                .path(queryId.toString())
                .path(slug)
                .path(String.valueOf(nextToken))
                .replaceQuery("")
                .build();
    }

    private static Column createColumn(String name, Type type)
    {
        TypeSignature signature = type.getTypeSignature();
        return new Column(name, signature.toString(), toClientTypeSignature(signature));
    }

    public static ClientTypeSignature toClientTypeSignature(TypeSignature signature)
    {
        return new ClientTypeSignature(signature.getBase(), signature.getParameters().stream()
                .map(Query::toClientTypeSignatureParameter)
                .collect(toImmutableList()));
    }

    private static ClientTypeSignatureParameter toClientTypeSignatureParameter(TypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case TYPE:
                return ClientTypeSignatureParameter.ofType(toClientTypeSignature(parameter.getTypeSignature()));
            case NAMED_TYPE:
                return ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(
                        parameter.getNamedTypeSignature().getFieldName().map(value ->
                                new RowFieldName(value.getName(), value.isDelimited())),
                        toClientTypeSignature(parameter.getNamedTypeSignature().getTypeSignature())));
            case LONG:
                return ClientTypeSignatureParameter.ofLong(parameter.getLongLiteral());
        }
        throw new IllegalArgumentException("Unsupported kind: " + parameter.getKind());
    }

    private StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        //Dont print any more stats for Async Query,
        StageInfo outputStage = queryInfo.isRunningAsync() ? null : queryInfo.getOutputStage().orElse(null);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes(outputStage, false).size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setSpilledBytes(queryStats.getSpilledDataSize().toBytes())
                .setSpilledReadTimeMillis(queryStats.getSpilledReadTime().toMillis())
                .setSpilledWriteTimeMillis(queryStats.getSpilledWriteTime().toMillis())
                .setSpilledNodes(globalUniqueNodes(outputStage, true).size())
                .setRootStage(toStageStats(outputStage))
                .setSnapshotStats(toSnapshotStats(queryInfo.getQueryId()))
                .build();
    }

    private SnapshotStats toSnapshotStats(QueryId queryId)
    {
        if (!isSnapshotEnabled(session) || queryId == null) {
            return null;
        }
        AtomicLong totalCpuTimeMillis = new AtomicLong(0L);
        AtomicLong lastSnapshotCpuTimeMillis = new AtomicLong(0L);
        AtomicLong allSnapshotsSizeBytes = new AtomicLong(0L);
        AtomicLong lastSnapshotSizeBytes = new AtomicLong(0L);
        AtomicLong totalWallTimeMillis = new AtomicLong(0L);
        AtomicLong lastWallTimeMillis = new AtomicLong(0L);
        QuerySnapshotManager querySnapshotManager = queryManager.getQuerySnapshotManager(queryId);
        if (querySnapshotManager != null) {
            long lastSnapshotId = querySnapshotManager.collectSnapshotCaptureStats(eachSize -> (eachWallTime, eachCpuTime) -> {
                allSnapshotsSizeBytes.addAndGet(eachSize);
                totalWallTimeMillis.addAndGet(eachWallTime);
                totalCpuTimeMillis.addAndGet(eachCpuTime);
            }, lastSize -> (lastWallTime, lastCpuTime) -> {
                lastSnapshotSizeBytes.set(lastSize);
                lastSnapshotCpuTimeMillis.set(lastCpuTime);
                lastWallTimeMillis.set(lastWallTime);
            });
            if (lastSnapshotId > 0) {
                SnapshotStats.Builder builder = SnapshotStats.builder();
                builder.setLastCaptureSnapshotId(lastSnapshotId)
                        .setAllSnapshotsSizeBytes(allSnapshotsSizeBytes.get())
                        .setLastSnapshotSizeBytes(lastSnapshotSizeBytes.get())
                        .setTotalWallTimeMillis(totalWallTimeMillis.get())
                        .setLastSnapshotWallTimeMillis(lastWallTimeMillis.get())
                        .setTotalCpuTimeMillis(totalCpuTimeMillis.get())
                        .setLastSnapshotCpuTimeMillis(lastSnapshotCpuTimeMillis.get());

                // Restore stats
                AtomicLong totalRestoreWallTime = new AtomicLong(0L);
                AtomicLong totalRestoreCpuTime = new AtomicLong(0L);
                AtomicLong totalRestoreSize = new AtomicLong(0L);
                long lastRestoreSnapshotId = 0;
                int restoreCount = 0;
                List<RestoreResult> restoreStats = querySnapshotManager.getRestoreStats();
                restoreCount = restoreStats.size();
                // Add restore stats if restore is happened
                if (restoreCount > 0) {
                    lastRestoreSnapshotId = restoreStats.get(restoreCount - 1).getSnapshotId();
                    restoreStats.forEach(restoreResult -> {
                        SnapshotInfo info = restoreResult.getSnapshotInfo();
                        totalRestoreWallTime.addAndGet(info.getEndTime() - info.getBeginTime());
                        totalRestoreCpuTime.addAndGet(info.getCpuTime());
                        totalRestoreSize.addAndGet(info.getSizeBytes());
                    });
                    builder.setSuccessRestoreCount(restoreCount)
                            .setLastRestoreSnapshotId(lastRestoreSnapshotId)
                            .setTotalRestoreWallTime(totalRestoreWallTime.get())
                            .setTotalRestoreCpuTime(totalRestoreCpuTime.get())
                            .setTotalRestoreSize(totalRestoreSize.get());
                }
                return builder.build();
            }
        }
        return null;
    }

    private static StageStats toStageStats(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return null;
        }

        io.prestosql.execution.StageStats stageStats = stageInfo.getStageStats();

        ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
        for (StageInfo subStage : stageInfo.getSubStages()) {
            subStages.add(toStageStats(subStage));
        }

        Set<String> uniqueNodes = new HashSet<>();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
        }

        return StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(stageInfo.getState().toString())
                .setRestoring(stageInfo.isRestoring())
                .setDone(stageInfo.getState().isDone())
                .setNodes(uniqueNodes.size())
                .setTotalSplits(stageStats.getTotalDrivers())
                .setQueuedSplits(stageStats.getQueuedDrivers())
                .setRunningSplits(stageStats.getRunningDrivers() + stageStats.getBlockedDrivers())
                .setCompletedSplits(stageStats.getCompletedDrivers())
                .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageStats.getRawInputPositions())
                .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                .setSubStages(subStages.build())
                .build();
    }

    public static Set<String> globalUniqueNodes(StageInfo stageInfo, boolean getSpilledNodes)
    {
        boolean isSpilledNode = false;

        if (stageInfo == null) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
        for (TaskInfo task : stageInfo.getTasks()) {
            if (getSpilledNodes) {
                for (PipelineStats pipeline : task.getStats().getPipelines()) {
                    isSpilledNode = pipeline.getOperatorSummaries().stream().filter(operatorStats -> operatorStats.getSpilledDataSize().toBytes() > 0).collect(Collectors.toList()).size() > 0;
                    if (isSpilledNode) {
                        break;
                    }
                }
            }
            // todo add nodeId to TaskInfo
            if (isSpilledNode || !getSpilledNodes) {
                URI uri = task.getTaskStatus().getSelf();
                nodes.add(uri.getHost() + ":" + uri.getPort());
            }
        }

        for (StageInfo subStage : stageInfo.getSubStages()) {
            nodes.addAll(globalUniqueNodes(subStage, getSpilledNodes));
        }
        return nodes.build();
    }

    private static URI findCancelableLeafStage(QueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
    }

    private static URI findCancelableLeafStage(StageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getState().isDone()) {
            return null;
        }

        // attempt to find a cancelable sub stage
        // check in reverse order since build side of a join will be later in the list
        for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
            URI leafStage = findCancelableLeafStage(subStage);
            if (leafStage != null) {
                return leafStage;
            }
        }

        // no matching sub stage, so return this stage
        return stage.getSelf();
    }

    private static QueryError toQueryError(QueryInfo queryInfo)
    {
        QueryState state = queryInfo.getState();
        if (state != FAILED) {
            return null;
        }

        ExecutionFailureInfo executionFailure;
        if (queryInfo.getFailureInfo() != null) {
            executionFailure = queryInfo.getFailureInfo();
        }
        else {
            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
            executionFailure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state)));
        }
        FailureInfo failure = executionFailure.toFailureInfoWithoutStack();

        ErrorCode errorCode;
        if (queryInfo.getErrorCode() != null) {
            errorCode = queryInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            log.warn("Failed query %s has no error code", queryInfo.getQueryId());
        }
        return new QueryError(
                firstNonNull(failure.getMessage(), "Internal error"),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                executionFailure.getSemanticErrorCode().map(SemanticErrorCode::name),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);
    }

    private static Warning toClientWarning(PrestoWarning warning)
    {
        WarningCode code = warning.getWarningCode();
        return new Warning(new Warning.Code(code.getCode(), code.getName()), warning.getMessage());
    }
}
