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
package io.prestosql.client.util;

import com.google.common.base.Joiner;
import io.airlift.json.JsonCodec;
import io.prestosql.client.ClientCapabilities;
import io.prestosql.client.ClientException;
import io.prestosql.client.ClientSelectedRole;
import io.prestosql.client.CrossRegionDynamicFilterRequest;
import io.prestosql.client.DataCenterClientSession;
import io.prestosql.client.DataCenterRequest;
import io.prestosql.client.DataCenterResponseType;
import io.prestosql.client.PrestoHeaders;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
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
import static io.prestosql.client.PrestoHeaders.PRESTO_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_CAPABILITIES;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static io.prestosql.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static io.prestosql.client.PrestoHeaders.PRESTO_LANGUAGE;
import static io.prestosql.client.PrestoHeaders.PRESTO_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static io.prestosql.client.PrestoHeaders.PRESTO_RESOURCE_ESTIMATE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_SOURCE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TRACE_TOKEN;
import static io.prestosql.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;

public class HttpUtil
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    private static final String ACCEPT_ENCODING_HEADER = "Accept-Encoding";
    private static final String ROOT_URL = "/v1/dc/statement/";
    private static final String DYNAMIC_FILTER_URL = "/v1/dc/filter/";
    private static final String USER_AGENT_VALUE = "DataCenterClient/" +
            firstNonNull(HttpUtil.class.getPackage().getImplementationVersion(), "unknown");
    private static final String CLIENT_CAPABILITIES = Joiner.on(",").join(ClientCapabilities.values());
    private static final JsonCodec<DataCenterRequest> DATA_CENTER_REQUEST_CODEC = jsonCodec(DataCenterRequest.class);
    private static final JsonCodec<CrossRegionDynamicFilterRequest> CRDF_REQUEST_CODEC = jsonCodec(CrossRegionDynamicFilterRequest.class);

    private HttpUtil()
    {
    }

    public static Request buildQueryRequest(String clientId, DataCenterClientSession session, String queryId, String query)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath(ROOT_URL + queryId).build();

        DataCenterRequest request = new DataCenterRequest(queryId,
                clientId, query,
                session.getMaxAnticipatedDelay(),
                DataCenterResponseType.HTTP_PULL);

        Request.Builder builder = prepareRequest(url, session)
                .post(RequestBody.create(MEDIA_TYPE_JSON, DATA_CENTER_REQUEST_CODEC.toJsonBytes(request)));

        if (session.getSource() != null) {
            builder.addHeader(PRESTO_SOURCE, session.getSource());
        }

        session.getTraceToken().ifPresent(token -> builder.addHeader(PRESTO_TRACE_TOKEN, token));

        if (session.getClientTags() != null && !session.getClientTags().isEmpty()) {
            builder.addHeader(PRESTO_CLIENT_TAGS, Joiner.on(",").join(session.getClientTags()));
        }
        if (session.getClientInfo() != null) {
            builder.addHeader(PRESTO_CLIENT_INFO, session.getClientInfo());
        }
        if (session.getCatalog() != null) {
            builder.addHeader(PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.addHeader(PRESTO_SCHEMA, session.getSchema());
        }
        if (session.getPath() != null) {
            builder.addHeader(PRESTO_PATH, session.getPath());
        }
        builder.addHeader(PRESTO_TIME_ZONE, session.getTimeZone().getId());
        if (session.getLocale() != null) {
            builder.addHeader(PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getProperties();
        for (Map.Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PRESTO_SESSION, entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, String> resourceEstimates = session.getResourceEstimates();
        for (Map.Entry<String, String> entry : resourceEstimates.entrySet()) {
            builder.addHeader(PRESTO_RESOURCE_ESTIMATE, entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, ClientSelectedRole> roles = session.getRoles();
        for (Map.Entry<String, ClientSelectedRole> entry : roles.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_ROLE, entry.getKey() + '=' + urlEncode(entry.getValue().toString()));
        }

        Map<String, String> extraCredentials = session.getExtraCredentials();
        for (Map.Entry<String, String> entry : extraCredentials.entrySet()) {
            builder.addHeader(PRESTO_EXTRA_CREDENTIAL, entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Map.Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(PRESTO_PREPARED_STATEMENT, urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        builder.addHeader(PRESTO_TRANSACTION_ID, session.getTransactionId() == null ? "NONE" : session.getTransactionId());

        builder.addHeader(PRESTO_CLIENT_CAPABILITIES, CLIENT_CAPABILITIES);

        return builder.build();
    }

    public static Request buildDynamicFilterRequest(String clientId, DataCenterClientSession session, String queryId, Map<String, byte[]> dynamicFilters)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath(DYNAMIC_FILTER_URL + queryId).build();

        CrossRegionDynamicFilterRequest request = new CrossRegionDynamicFilterRequest(queryId, clientId, dynamicFilters);

        Request.Builder builder = prepareRequest(url, session)
                .post(RequestBody.create(MEDIA_TYPE_JSON, CRDF_REQUEST_CODEC.toJsonBytes(request)));

        return builder.build();
    }

    public static Request.Builder prepareRequest(HttpUrl url, DataCenterClientSession session)
    {
        Request.Builder builder = new Request.Builder();
        builder.addHeader(PRESTO_USER, session.getUser())
                .addHeader(USER_AGENT, USER_AGENT_VALUE);
        builder.addHeader(ACCEPT_ENCODING_HEADER, "");
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
        return builder.url(url);
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
}
