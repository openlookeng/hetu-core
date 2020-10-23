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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.prestosql.client.JsonResponse;
import io.prestosql.execution.Input;
import io.prestosql.execution.QueryStats;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

public class QueryInfoClient
{
    private static final Logger LOG = LoggerFactory.getLogger(QueryInfoClient.class);

    private static final String USER_AGENT_VALUE = QueryInfoClient.class.getSimpleName() +
            "/" +
            firstNonNull(QueryInfoClient.class.getPackage().getImplementationVersion(), "unknown");
    private static final JsonCodec<BasicQueryInfo> QUERY_INFO_CODEC = jsonCodec(BasicQueryInfo.class);

    private final OkHttpClient okHttpClient;

    public QueryInfoClient(OkHttpClient okHttpClient)
    {
        this.okHttpClient = okHttpClient;
    }

    public BasicQueryInfo from(URI infoUri, String id)
    {
        infoUri = requireNonNull(infoUri, "infoUri is null");
        HttpUrl url = HttpUrl.get(infoUri);
        url = url.newBuilder().encodedPath("/v1/query/" + id).query(null).build();

        Request.Builder request = new Request.Builder()
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .url(url);

        JsonResponse<BasicQueryInfo> response = JsonResponse.execute(QUERY_INFO_CODEC, okHttpClient, request.build());
        if ((response.getStatusCode() != HTTP_OK) || !response.hasValue()) {
            if (response.getStatusCode() != Response.Status.GONE.getStatusCode()) {
                LOG.warn("Error while getting query info! {}", response.getStatusMessage());
            }
            return null;
        }
        return response.getValue();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BasicQueryInfo
    {
        private final QueryStats queryStats;
        private final Set<Input> inputs;

        @JsonCreator
        public BasicQueryInfo(
                @JsonProperty("queryStats") QueryStats queryStats,
                @JsonProperty("inputs") Set<Input> inputs)
        {
            this.queryStats = queryStats;
            this.inputs = inputs;
        }

        @JsonProperty
        public QueryStats getQueryStats()
        {
            return queryStats;
        }

        @JsonProperty
        public Set<Input> getInputs()
        {
            return inputs;
        }
    }
}
