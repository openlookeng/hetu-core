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
package io.hetu.core.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.hetu.core.plugin.mongodb.MongoQueryRunner.createMongoQueryRunner;

@Test
public class TestMongoDistributedQueries
        extends AbstractTestDistributedQueries
{
    private MongoServer server;

    public TestMongoDistributedQueries()
    {
        this(new MongoServer());
    }

    public TestMongoDistributedQueries(MongoServer mongoServer)
    {
        super(() -> createMongoQueryRunner(mongoServer, ImmutableMap.of(), TpchTable.getTables()));
        this.server = mongoServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testRenameTable()
    {
        // the connector does not support renaming tables
    }

    @Override
    public void testRenameColumn()
    {
        // the connector does not support renaming columns
    }

    @Override
    public void testDropColumn()
    {
        // the connector does not support dropping columns
    }

    @Override
    public void testDelete()
    {
        // the connector does not support delete
    }

    @Override
    public void testCommentTable()
    {
        // the connector does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }
}
