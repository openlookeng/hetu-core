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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableMap;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static io.airlift.tpch.TpchTable.ORDERS;
import static io.hetu.core.plugin.datacenter.DataCenterQueryRunner.createDCQueryRunner;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestDCIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPrestoServer hetuServer;

    public TestDCIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPrestoServer(
                ImmutableMap.<String, String>builder().put("node-scheduler.include-coordinator", "true").build()));
    }

    public TestDCIntegrationSmokeTest(TestingPrestoServer hetuServer)
    {
        super(() -> createDCQueryRunner(hetuServer, ORDERS));
        this.hetuServer = hetuServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        hetuServer.close();
    }

    @Test
    public void testShow()
    {
        List<MaterializedRow> rows = computeActual("show catalogs").getMaterializedRows();
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("dc.tpch")));

        assertEquals(computeActual("show schemas from dc.tpch").getMaterializedRows(),
                computeActual("show schemas from tpch").getMaterializedRows());
        assertEquals(computeActual("show tables from dc.tpch.tiny").getMaterializedRows(),
                computeActual("show tables from tpch.tiny").getMaterializedRows());
    }

    @Test
    public void testUse()
    {
        assertQuerySucceeds("use dc.tpch.tiny");
    }

    @Test
    public void testSelect()
    {
        assertEquals(computeActual("select count(*) from dc.tpch.tiny.orders").toString(),
                computeActual("select count(*) from tpch.tiny.orders").toString());

        List<MaterializedRow> actual = computeActual(
                "select * from dc.tpch.tiny.orders order by orderkey limit 10").getMaterializedRows();
        List<MaterializedRow> expect = computeActual(
                "select * from tpch.tiny.orders order by orderkey limit 10").getMaterializedRows();

        assertEquals(actual, expect);
    }
}
