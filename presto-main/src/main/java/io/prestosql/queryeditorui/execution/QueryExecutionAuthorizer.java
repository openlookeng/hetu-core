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

import com.google.common.collect.ImmutableSet;
import io.prestosql.queryeditorui.protocol.Table;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class QueryExecutionAuthorizer
{
    private final String user;
    private final String defaultConnector;
    private final String defaultSchema;

    public QueryExecutionAuthorizer(String user, String defaultConnector, String defaultSchema)
    {
        this.user = requireNonNull(user);
        this.defaultConnector = requireNonNull(defaultConnector);
        this.defaultSchema = requireNonNull(defaultSchema);
    }

    public boolean isAuthorizedWrite(String connectorId, String schema, String table)
    {
        //TODO
        return true;
    }

    public boolean isAuthorizedRead(Set<Table> tables)
    {
        //TODO
        return true;
    }

    public static Set<Table> tablesUsedByQuery(String query, String defaultConnector, String defaultSchema)
    {
        return ImmutableSet.of();
    }

    public Set<Table> tablesUsedByQuery(String query)
    {
        return tablesUsedByQuery(query, defaultConnector, defaultSchema);
    }
}
