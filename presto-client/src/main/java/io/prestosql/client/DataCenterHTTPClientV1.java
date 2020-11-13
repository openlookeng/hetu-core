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
package io.prestosql.client;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.client.block.ExternalBlockEncodingSerde;
import io.prestosql.client.protocol.DataCenterRowIterable;
import io.prestosql.client.util.HttpUtil;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.TypeManager;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_ROLE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static io.prestosql.client.util.HttpUtil.buildDynamicFilterRequest;
import static io.prestosql.client.util.HttpUtil.prepareRequest;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DataCenterHTTPClientV1
        implements DataCenterStatementClient
{
    private static final Logger log = Logger.get(DataCenterHTTPClientV1.class);

    private static final String ROOT_URL = "/v1/dc/statement/";
    private static final JsonCodec<DataCenterResponse> DATA_CENTER_RESPONSE_JSON_CODEC = jsonCodec(DataCenterResponse.class);
    private static final JsonCodec<DataCenterQueryResults> DATA_CENTER_QUERY_RESULTS_JSON_CODEC = jsonCodec(DataCenterQueryResults.class);
    private static final JsonCodec<CrossRegionDynamicFilterResponse> CRDF_RESPONSE_JSON_CODEC = jsonCodec(CrossRegionDynamicFilterResponse.class);
    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();

    private final OkHttpClient httpClient;
    private final String query;
    private final AtomicReference<DataCenterQueryResults> currentResults = new AtomicReference<>();
    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final AtomicReference<String> setPath = new AtomicReference<>();
    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final Map<String, ClientSelectedRole> setRoles = new ConcurrentHashMap<>();
    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();
    private final AtomicReference<String> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();
    private final ZoneId timeZone;
    private final Duration requestTimeoutNanos;
    private final String slug;
    private final String queryId;
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
    private final HttpUrl serverURI;
    private final HttpUrl cancelUrl;
    private final String clientId;

    private long token;
    private final PagesSerde serde;
    private final DataCenterClientSession session;
    private TypeManager typeManager;

    public DataCenterHTTPClientV1(OkHttpClient httpClient, DataCenterClientSession session, String query, String queryId)
    {
        this.session = requireNonNull(session, "session is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.query = requireNonNull(query, "query is null");
        this.timeZone = session.getTimeZone();
        this.requestTimeoutNanos = session.getClientRequestTimeout();
        this.clientId = UUID.randomUUID().toString();
        this.serverURI = HttpUrl.get(session.getServer());
        if (this.serverURI == null) {
            throw new RuntimeException("Invalid server Url:" + session.getServer());
        }
        this.typeManager = session.getTypeManager();
        this.serde = new PagesSerdeFactory(new ExternalBlockEncodingSerde(this.typeManager),
                true).createPagesSerde();

        // Submit the query
        DataCenterResponse result = null;
        int attempts = 0;
        while (attempts <= 10) {
            Request request = HttpUtil.buildQueryRequest(this.clientId, session, queryId, query);
            try {
                if (attempts > 0) {
                    try {
                        MILLISECONDS.sleep(attempts * 10);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException("Could not establish the connection");
                    }
                }
                JsonResponse<DataCenterResponse> response = JsonResponse.execute(DATA_CENTER_RESPONSE_JSON_CODEC, httpClient, request);
                if ((response.getStatusCode() != HTTP_OK) || !response.hasValue()) {
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw requestFailedException("starting query", request, response);
                }
                result = response.getValue();
                break;
            }
            catch (UncheckedIOException ex) {
                log.debug("Failed submitting query. Retrying...", ex);
            }
            attempts++;
        }
        if (result == null) {
            throw new RuntimeException("received null response from data center");
        }
        if (result.getState() == DataCenterResponse.State.FINISHED_ALREADY) {
            this.slug = null;
            this.cancelUrl = null;
            state.compareAndSet(State.RUNNING, State.FINISHED);
        }
        else {
            // isQueryFinishedByOtherSplit = false;
            if (!result.isRegistered()) {
                state.compareAndSet(State.RUNNING, State.FINISHED);
            }
            this.slug = result.getSlug();
            this.cancelUrl = this.serverURI.newBuilder().encodedPath(ROOT_URL + this.queryId + "/" + this.slug).build();
        }
        this.currentResults.set(new DataCenterQueryResults(
                this.queryId,
                this.serverURI.uri(),
                null,
                this.state.get() == State.RUNNING ? URI.create("") : null,
                null,
                null,
                StatementStats.builder()
                        .setState(state.toString())
                        .setQueued(true)
                        .setElapsedTimeMillis(0)
                        .setQueuedTimeMillis(0)
                        .build(),
                null,
                ImmutableList.of(),
                null,
                true));
    }

    private HttpUrl nextURL()
    {
        return this.serverURI.newBuilder().encodedPath(ROOT_URL + DataCenterResponseType.HTTP_PULL + "/" + this.clientId + "/" + this.queryId + "/" + this.slug + "/" + this.token).build();
    }

    @Override
    public String getQuery()
    {
        return this.query;
    }

    @Override
    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    @Override
    public boolean isRunning()
    {
        return state.get() == State.RUNNING;
    }

    @Override
    public boolean isClientAborted()
    {
        return state.get() == State.CLIENT_ABORTED;
    }

    @Override
    public boolean isClientError()
    {
        return state.get() == State.CLIENT_ERROR;
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == State.FINISHED;
    }

    @Override
    public StatementStats getStats()
    {
        return currentResults.get().getStats();
    }

    @Override
    public QueryStatusInfo currentStatusInfo()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        return currentResults.get();
    }

    @Override
    public List<Page> getPages()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        List<Page> pages = new ArrayList<>();
        List<SerializedPage> dcSerializedPages = currentResults.get().getData();
        if (dcSerializedPages == null) {
            return pages;
        }
        for (SerializedPage dcSerializedPage : dcSerializedPages) {
            pages.add(serde.deserialize(dcSerializedPage));
        }
        return pages;
    }

    @Override
    public QueryData currentData()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        return new RowQueryData(this.getPages(), this.currentResults.get().getColumns(), this.typeManager);
    }

    private static class RowQueryData
            implements QueryData
    {
        private Iterable<List<Object>> data;

        private RowQueryData(List<Page> pages, List<Column> columns, TypeManager typeManager)
        {
            if (pages != null && columns != null) {
                ImmutableList.Builder<DataCenterRowIterable> rows = ImmutableList.builder();
                long rowsCount = 0;
                for (Page page : pages) {
                    rowsCount += page.getPositionCount();
                    rows.add(new DataCenterRowIterable(null, columns, page, typeManager));
                }
                if (rowsCount > 0) {
                    // client implementations do not properly handle empty list of data
                    this.data = Iterables.concat(rows.build());
                }
            }
        }

        @Override
        public Iterable<List<Object>> getData()
        {
            return this.data;
        }
    }

    @Override
    public QueryStatusInfo finalStatusInfo()
    {
        checkState(!isRunning(), "current position is still valid");
        return currentResults.get();
    }

    @Override
    public Optional<String> getSetCatalog()
    {
        return Optional.ofNullable(setCatalog.get());
    }

    @Override
    public Optional<String> getSetSchema()
    {
        return Optional.ofNullable(setSchema.get());
    }

    @Override
    public Optional<String> getSetPath()
    {
        return Optional.ofNullable(setPath.get());
    }

    @Override
    public Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    @Override
    public Set<String> getResetSessionProperties()
    {
        return ImmutableSet.copyOf(resetSessionProperties);
    }

    @Override
    public Map<String, ClientSelectedRole> getSetRoles()
    {
        return ImmutableMap.copyOf(setRoles);
    }

    @Override
    public Map<String, String> getAddedPreparedStatements()
    {
        return ImmutableMap.copyOf(addedPreparedStatements);
    }

    @Override
    public Set<String> getDeallocatedPreparedStatements()
    {
        return ImmutableSet.copyOf(deallocatedPreparedStatements);
    }

    @Nullable
    @Override
    public String getStartedTransactionId()
    {
        return startedTransactionId.get();
    }

    @Override
    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    @Override
    public boolean advance()
    {
        if (!isRunning()) {
            return false;
        }

        QueryStatusInfo queryStatusInfo = currentStatusInfo();
        if (queryStatusInfo.getNextUri() == null) {
            if (queryStatusInfo.getStats().getState().equals("FAILED")) {
                state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                log.error("fetching next result failed.");
                throw new RuntimeException("fetching next result failed.");
            }
            state.compareAndSet(State.RUNNING, State.FINISHED);
            return false;
        }
        Request request = prepareRequest(this.nextURL(), this.session).build();

        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        while (true) {
            if (isClientAborted()) {
                return false;
            }

            Duration sinceStart = Duration.nanosSince(start);
            if (attempts > 0 && sinceStart.compareTo(requestTimeoutNanos) > 0) {
                // requestTimeoutNanos is used for internal purpose
                // The client retries upt to a maximum of requestTimeoutNanos and then fail
                // This value is controlled by the ClientSession#clientRequestTimeout
                state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                throw new RuntimeException(format("Error fetching next (attempts: %s, duration: %s)", attempts, sinceStart), cause);
            }

            if (attempts > 0) {
                // back-off on retry
                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException e) {
                    try {
                        close();
                    }
                    finally {
                        Thread.currentThread().interrupt();
                    }
                    //todo: 02/12 : add DataResponse state
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
            }
            attempts++;

            JsonResponse<DataCenterQueryResults> response;
            try {
                response = JsonResponse.execute(DATA_CENTER_QUERY_RESULTS_JSON_CODEC, httpClient, request);
            }
            catch (RuntimeException e) {
                // If there is a timeout, it will be SocketTimeoutException
                cause = e;
                continue;
            }

            if ((response.getStatusCode() == HTTP_OK) && response.hasValue()) {
                processResponse(response.getHeaders(), response.getValue());
                return true;
            }

            if (response.getStatusCode() != HTTP_UNAVAILABLE) {
                state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                log.error("response.getStatusCode=%s", response.getStatusCode());
                if (response.getException() != null) {
                    throw new RuntimeException("fetching next result: " + response.toString(), response.getException());
                }
                else {
                    throw new RuntimeException("fetching next result: " + response.toString());
                }
            }
        }
    }

    private void processResponse(Headers headers, DataCenterQueryResults results)
    {
        this.token++;
        if (results.getUseHeaderInformation()) {
            setCatalog.set(headers.get(PRESTO_SET_CATALOG));
            setSchema.set(headers.get(PRESTO_SET_SCHEMA));
            setPath.set(headers.get(PRESTO_SET_PATH));

            for (String setSession : headers.values(PRESTO_SET_SESSION)) {
                List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
                if (keyValue.size() != 2) {
                    continue;
                }
                setSessionProperties.put(keyValue.get(0), urlDecode(keyValue.get(1)));
            }
            resetSessionProperties.addAll(headers.values(PRESTO_CLEAR_SESSION));

            for (String setRole : headers.values(PRESTO_SET_ROLE)) {
                List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setRole);
                if (keyValue.size() != 2) {
                    continue;
                }
                setRoles.put(keyValue.get(0), ClientSelectedRole.valueOf(urlDecode(keyValue.get(1))));
            }

            for (String entry : headers.values(PRESTO_ADDED_PREPARE)) {
                List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(entry);
                if (keyValue.size() != 2) {
                    continue;
                }
                addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
            }
            for (String entry : headers.values(PRESTO_DEALLOCATED_PREPARE)) {
                deallocatedPreparedStatements.add(urlDecode(entry));
            }

            String startedTransactionId = headers.get(PRESTO_STARTED_TRANSACTION_ID);
            if (startedTransactionId != null) {
                this.startedTransactionId.set(startedTransactionId);
            }
            if (headers.get(PRESTO_CLEAR_TRANSACTION_ID) != null) {
                clearTransactionId.set(true);
            }
        }
        currentResults.set(results);
    }

    @Override
    public void cancelLeafStage()
    {
        checkState(!isClientAborted(), "client is closed");
        URI uri = currentStatusInfo().getPartialCancelUri();
        if (uri != null) {
            httpDelete(uri);
        }
    }

    @Override
    public boolean applyDynamicFilters(Map<String, byte[]> dynamicFilters)
    {
        Request request = buildDynamicFilterRequest(this.clientId, session, queryId, dynamicFilters);
        JsonResponse<CrossRegionDynamicFilterResponse> response = JsonResponse.execute(CRDF_RESPONSE_JSON_CODEC, httpClient, request);
        if ((response.getStatusCode() != HTTP_OK) || !response.hasValue() || !response.getValue().getApplied()) {
            return false;
        }
        return true;
    }

    @Override
    public void close()
    {
        // If the query is not done, abort the query.
        if (state.compareAndSet(DataCenterHTTPClientV1.State.RUNNING, DataCenterHTTPClientV1.State.CLIENT_ABORTED)) {
            URI uri = this.cancelUrl.uri();
            if (uri != null) {
                httpDelete(uri);
            }
        }
    }

    private void httpDelete(URI uri)
    {
        HttpUrl httpUrl = HttpUrl.get(uri);
        if (httpUrl == null) {
            throw new RuntimeException("Invalid URL:" + uri.toString());
        }
        Request request = prepareRequest(httpUrl, this.session)
                .delete()
                .build();
        try {
            httpClient.newCall(request)
                    .execute()
                    .close();
        }
        catch (IOException e) {
            // callers expect this method not to throw
            log.debug("Failed to execute delete request: " + request, e);
        }
    }

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private enum State
    {
        /**
         * submitted to server, not in terminal state (including planning, queued, running, etc)
         */
        RUNNING,
        CLIENT_ERROR,
        CLIENT_ABORTED,
        /**
         * finished on remote Hetu server (including failed and successfully completed)
         */
        FINISHED,
    }
}
