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
package io.prestosql.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_CSP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_CSP_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_RP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_RP_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XFO;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XFO_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XXP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XXP_VALUE;
import static io.prestosql.client.PrestoHeaders.PRESTO_BATCH_QUERY;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
class StatementClientV1
        implements StatementClient
{
    private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = StatementClientV1.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClientV1.class.getPackage().getImplementationVersion(), "unknown");

    private final OkHttpClient httpClient;
    private final String query;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
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
    private final String user;
    private final String clientCapabilities;
    private final boolean timeInMilliseconds;
    private boolean isBatchQuery;
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    public StatementClientV1(OkHttpClient httpClient, ClientSession session, String query, boolean isBatchQuery)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = httpClient;
        this.timeZone = session.getTimeZone();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout();
        this.user = session.getUser();
        this.clientCapabilities = Joiner.on(",").join(ClientCapabilities.values());
        this.timeInMilliseconds = session.isTimeInMilliseconds();
        this.isBatchQuery = isBatchQuery;

        Request request = buildQueryRequest(session, query);

        JsonResponse<QueryResults> response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request);
        if ((response.getStatusCode() != HTTP_OK) || !response.hasValue()) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            throw requestFailedException("starting query", request, response);
        }

        processResponse(response.getHeaders(), response.getValue());
    }

    private Request buildQueryRequest(ClientSession session, String query)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath("/v1/statement").build();

        Request.Builder builder = prepareRequest(url)
                .post(RequestBody.create(MEDIA_TYPE_TEXT, query));

        if (session.getSource() != null) {
            builder.addHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
        }

        session.getTraceToken().ifPresent(token -> builder.addHeader(PrestoHeaders.PRESTO_TRACE_TOKEN, token));

        if (session.getClientTags() != null && !session.getClientTags().isEmpty()) {
            builder.addHeader(PrestoHeaders.PRESTO_CLIENT_TAGS, Joiner.on(",").join(session.getClientTags()));
        }
        if (session.getClientInfo() != null) {
            builder.addHeader(PrestoHeaders.PRESTO_CLIENT_INFO, session.getClientInfo());
        }
        if (session.getCatalog() != null) {
            builder.addHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.addHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        if (session.getPath() != null) {
            builder.addHeader(PrestoHeaders.PRESTO_PATH, session.getPath());
        }
        builder.addHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZone().getId());
        if (session.getLocale() != null) {
            builder.addHeader(PrestoHeaders.PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getProperties();
        for (Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_SESSION, entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, String> resourceEstimates = session.getResourceEstimates();
        for (Entry<String, String> entry : resourceEstimates.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_RESOURCE_ESTIMATE, entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, ClientSelectedRole> roles = session.getRoles();
        for (Entry<String, ClientSelectedRole> entry : roles.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_ROLE, entry.getKey() + '=' + urlEncode(entry.getValue().toString()));
        }

        Map<String, String> extraCredentials = session.getExtraCredentials();
        for (Entry<String, String> entry : extraCredentials.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_EXTRA_CREDENTIAL, entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_PREPARED_STATEMENT, urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        builder.addHeader(PrestoHeaders.PRESTO_TRANSACTION_ID, session.getTransactionId() == null ? "NONE" : session.getTransactionId());

        builder.addHeader(PrestoHeaders.PRESTO_CLIENT_CAPABILITIES, clientCapabilities);

        // add security header
        if (System.getProperty(HTTP_SECURITY_CSP) != null) {
            builder.addHeader(HTTP_SECURITY_CSP, System.getProperty(HTTP_SECURITY_CSP));
        }
        else {
            builder.addHeader(HTTP_SECURITY_CSP, HTTP_SECURITY_CSP_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_RP) != null) {
            builder.addHeader(HTTP_SECURITY_RP, System.getProperty(HTTP_SECURITY_RP));
        }
        else {
            builder.addHeader(HTTP_SECURITY_RP, HTTP_SECURITY_RP_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XCTO) != null) {
            builder.addHeader(HTTP_SECURITY_XCTO, System.getProperty(HTTP_SECURITY_XCTO));
        }
        else {
            builder.addHeader(HTTP_SECURITY_XCTO, HTTP_SECURITY_XCTO_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XFO) != null) {
            builder.addHeader(HTTP_SECURITY_XFO, System.getProperty(HTTP_SECURITY_XFO));
        }
        else {
            builder.addHeader(HTTP_SECURITY_XFO, HTTP_SECURITY_XFO_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XPCDP) != null) {
            builder.addHeader(HTTP_SECURITY_XPCDP, System.getProperty(HTTP_SECURITY_XPCDP));
        }
        else {
            builder.addHeader(HTTP_SECURITY_XPCDP, HTTP_SECURITY_XPCDP_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XXP) != null) {
            builder.addHeader(HTTP_SECURITY_XXP, System.getProperty(HTTP_SECURITY_XXP));
        }
        else {
            builder.addHeader(HTTP_SECURITY_XXP, HTTP_SECURITY_XXP_VALUE);
        }
        if (isBatchQuery) {
            builder.addHeader(PRESTO_BATCH_QUERY, "1");
        }

        return builder.build();
    }

    @Override
    public String getQuery()
    {
        return query;
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
    public QueryData currentData()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        return currentResults.get();
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

    @Override
    @Nullable
    public String getStartedTransactionId()
    {
        return startedTransactionId.get();
    }

    @Override
    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    private Request.Builder prepareRequest(HttpUrl url)
    {
        return new Request.Builder()
                .addHeader(PrestoHeaders.PRESTO_USER, user)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .url(url);
    }

    @Override
    public boolean advance()
    {
        if (!isRunning()) {
            return false;
        }

        URI nextUri = currentStatusInfo().getNextUri();
        if (nextUri == null) {
            state.compareAndSet(State.RUNNING, State.FINISHED);
            return false;
        }

        Request request = prepareRequest(HttpUrl.get(nextUri)).build();

        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        while (true) {
            if (isClientAborted()) {
                return false;
            }

            Duration sinceStart = Duration.nanosSince(start);
            if (attempts > 0 && sinceStart.compareTo(requestTimeoutNanos) > 0) {
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
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
            }
            attempts++;

            JsonResponse<QueryResults> response;
            try {
                response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request);
            }
            catch (RuntimeException e) {
                cause = e;
                continue;
            }

            if ((response.getStatusCode() == HTTP_OK) && response.hasValue()) {
                processResponse(response.getHeaders(), response.getValue());
                return true;
            }

            if (response.getStatusCode() != HTTP_UNAVAILABLE) {
                state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                throw requestFailedException("fetching next", request, response);
            }
        }
    }

    private void processResponse(Headers headers, QueryResults results)
    {
        setCatalog.set(headers.get(PrestoHeaders.PRESTO_SET_CATALOG));
        setSchema.set(headers.get(PrestoHeaders.PRESTO_SET_SCHEMA));
        setPath.set(headers.get(PrestoHeaders.PRESTO_SET_PATH));

        for (String setSession : headers.values(PrestoHeaders.PRESTO_SET_SESSION)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), urlDecode(keyValue.get(1)));
        }
        resetSessionProperties.addAll(headers.values(PrestoHeaders.PRESTO_CLEAR_SESSION));

        for (String setRole : headers.values(PrestoHeaders.PRESTO_SET_ROLE)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setRole);
            if (keyValue.size() != 2) {
                continue;
            }
            setRoles.put(keyValue.get(0), ClientSelectedRole.valueOf(urlDecode(keyValue.get(1))));
        }

        for (String entry : headers.values(PrestoHeaders.PRESTO_ADDED_PREPARE)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(entry);
            if (keyValue.size() != 2) {
                continue;
            }
            addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
        }
        for (String entry : headers.values(PrestoHeaders.PRESTO_DEALLOCATED_PREPARE)) {
            deallocatedPreparedStatements.add(urlDecode(entry));
        }

        String transactionId = headers.get(PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID);
        if (transactionId != null) {
            this.startedTransactionId.set(transactionId);
        }
        if (headers.get(PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID) != null) {
            clearTransactionId.set(true);
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
    public boolean isTimeInMilliseconds()
    {
        return timeInMilliseconds;
    }

    @Override
    public void close()
    {
        // If the query is not done, abort the query.
        if (state.compareAndSet(State.RUNNING, State.CLIENT_ABORTED)) {
            URI uri = currentResults.get().getNextUri();
            if (uri != null) {
                httpDelete(uri);
            }
        }
    }

    private void httpDelete(URI uri)
    {
        Request request = prepareRequest(HttpUrl.get(uri))
                .delete()
                .build();
        try {
            httpClient.newCall(request)
                    .execute()
                    .close();
        }
        catch (IOException ignored) {
            // callers expect this method not to throw
        }
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
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
         * finished on remote Presto server (including failed and successfully completed)
         */
        FINISHED,
    }
}
