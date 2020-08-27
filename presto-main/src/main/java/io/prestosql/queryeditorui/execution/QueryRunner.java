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

import io.prestosql.client.ClientSession;
import io.prestosql.client.StatementClient;
import io.prestosql.client.StatementClientFactory;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QueryRunner
        implements Closeable
{
    private final ClientSession session;
    private final OkHttpClient httpClient;
    private StatementClient currentClient;

    protected QueryRunner(ClientSession session, OkHttpClient httpClient)
    {
        this.session = requireNonNull(session, "session is null");
        this.httpClient = httpClient;
    }

    public synchronized StatementClient startInternalQuery(String query)
    {
        currentClient = StatementClientFactory.newStatementClient(httpClient, session, query);
        return currentClient;
    }

    public synchronized StatementClient getCurrentClient()
    {
        return currentClient;
    }

    public ClientSession getSession()
    {
        return session;
    }

    @Override
    public void close()
    {
    }

    public static class QueryRunnerFactory
    {
        private final ClientSessionFactory sessionFactory;
        private final OkHttpClient httpClient;

        public QueryRunnerFactory(ClientSessionFactory sessionFactory, OkHttpClient httpClient)
        {
            this.httpClient = httpClient;
            this.sessionFactory = sessionFactory;
        }

        public ClientSessionFactory getSessionFactory()
        {
            return sessionFactory;
        }

        public OkHttpClient getHttpClient()
        {
            return httpClient;
        }

        public QueryRunner create(String user, String catalog, String schema, Map<String, String> properties)
        {
            return new QueryRunner(sessionFactory.create(user, catalog, schema, properties), httpClient);
        }

        public QueryRunner create(ClientSession currentSession)
        {
            return new QueryRunner(currentSession, httpClient);
        }

        public QueryRunner create(String source, String user)
        {
            return new QueryRunner(sessionFactory.create(source, user), httpClient);
        }
    }
}
