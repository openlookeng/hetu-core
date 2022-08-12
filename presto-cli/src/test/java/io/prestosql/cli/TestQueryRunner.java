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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.client.ClientSession;
import io.prestosql.client.ClientTypeSignature;
import io.prestosql.client.Column;
import io.prestosql.client.QueryResults;
import io.prestosql.client.StatementStats;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.jline.terminal.Terminal;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.cli.ClientOptions.OutputFormat.CSV;
import static io.prestosql.client.ClientStandardTypes.BIGINT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.jline.terminal.TerminalBuilder.terminal;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestQueryRunner
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private MockWebServer server;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
    }

    @Test
    public void testCookie()
            throws Exception
    {
        server.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("/v1/statement"))
                .addHeader(SET_COOKIE, "a=apple"));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));

        QueryRunner queryRunner = createQueryRunner(createClientSession(server));

        try (Terminal terminal = terminal()) {
            try (Query query = queryRunner.startQuery("first query will introduce a cookie")) {
                query.renderOutput(terminal, nullPrintStream(), nullPrintStream(), CSV, false, false);
            }
            try (Query query = queryRunner.startQuery("second query should carry the cookie")) {
                query.renderOutput(terminal, nullPrintStream(), nullPrintStream(), CSV, false, false);
            }
        }

        assertNull(server.takeRequest().getHeader("Cookie"));
        assertEquals(server.takeRequest().getHeader("Cookie"), "a=apple");
        assertEquals(server.takeRequest().getHeader("Cookie"), "a=apple");
    }

    static ClientSession createClientSession(MockWebServer server)
    {
        return new ClientSession(
                server.url("/").uri(),
                "user",
                "source",
                Optional.empty(),
                ImmutableSet.of(),
                "clientInfo",
                "catalog",
                "schema",
                "path",
                ZoneId.of("America/Los_Angeles"),
                Locale.ENGLISH,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                null,
                new Duration(2, MINUTES));
    }

    static String createResults(MockWebServer server)
    {
        QueryResults queryResults = new QueryResults(
                "20160128_214710_00012_rk68b",
                server.url("/query.html?20160128_214710_00012_rk68b").uri(),
                null,
                null,
                ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))),
                ImmutableList.of(ImmutableList.of(123)),
                StatementStats.builder().setState("FINISHED").build(),
                null,
                ImmutableList.of(),
                null,
                null);
        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    static QueryRunner createQueryRunner(ClientSession clientSession)
    {
        return new QueryRunner(
                clientSession,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                false);
    }

    static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }
}
