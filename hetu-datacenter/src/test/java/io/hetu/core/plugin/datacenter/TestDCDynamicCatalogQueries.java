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
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.hetu.core.plugin.datacenter.DataCenterQueryRunner.createDCQueryRunner;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class TestDCDynamicCatalogQueries
{
    private final TestingPrestoServer hetuServer;

    private QueryRunner queryRunner;

    public TestDCDynamicCatalogQueries()
            throws Exception
    {
        this(new TestingPrestoServer(
                ImmutableMap.<String, String>builder().put("node-scheduler.include-coordinator", "true").build()));
    }

    public TestDCDynamicCatalogQueries(TestingPrestoServer hetuServer)
            throws Exception
    {
        this.queryRunner = createDCQueryRunner(hetuServer, ORDERS);
        this.hetuServer = hetuServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        hetuServer.close();
    }

    @Test
    public void testAddCatalogShow()
    {
        hetuServer.createCatalog("tpcm", "tpch");
        List<MaterializedRow> rows = computeActual("show catalogs").getMaterializedRows();
        assertTrue(rows.stream().anyMatch(row -> row.getField(0).equals("dc.tpcm")));

        assertEquals(computeActual("show schemas from dc.tpcm").getMaterializedRows(),
                computeActual("show schemas from tpch").getMaterializedRows(),
                "The show schemas query failed for the newly added catalog");
        assertEquals(computeActual("show tables from dc.tpcm.tiny").getMaterializedRows(),
                computeActual("show tables from tpch.tiny").getMaterializedRows(),
                "The show tables query failed for the newly added catalog");

        hetuServer.deleteCatalog("tpcm");
        List<MaterializedRow> rowsCatalogs = computeActual("show catalogs").getMaterializedRows();
        assertFalse(rowsCatalogs.stream().anyMatch(row -> row.getField(0).equals("dc.tpcm")));
        assertQueryFails("show schemas from dc.tpcm", "Catalog dc.tpcm does not exist");
    }

    @Test
    public void testAddCatalogUse()
    {
        hetuServer.createCatalog("tpcv", "tpch");
        assertQuerySucceeds("use dc.tpcv.tiny");
        hetuServer.deleteCatalog("tpcv");
        assertQueryFails("use dc.tpcv.tiny", "Catalog does not exist: dc.tpcv");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testAddCatalogSelect()
    {
        hetuServer.createCatalog("tpcn", "tpch");
        assertEquals(computeActual("select count(*) from dc.tpcn.tiny.orders").toString(),
                computeActual("select count(*) from tpch.tiny.orders").toString(), "The number of rows does not match");

        List<MaterializedRow> actual = computeActual(
                "select * from dc.tpcn.tiny.orders order by orderkey limit 10").getMaterializedRows();
        List<MaterializedRow> expect = computeActual(
                "select * from tpch.tiny.orders order by orderkey limit 10").getMaterializedRows();

        assertEquals(actual, expect, "The query results are not matching for both of the catalogs");

        hetuServer.deleteCatalog("tpcn");
        computeActual("select count(*) from dc.tpcn.tiny.orders");
        fail("Able to query from deleted catalog");
    }

    protected MaterializedResult computeActual(String sql)
    {
        return queryRunner.execute(queryRunner.getDefaultSession(), sql).toTestTypes();
    }

    protected void assertQuerySucceeds(String sql)
    {
        try {
            queryRunner.execute(queryRunner.getDefaultSession(), sql);
        }
        catch (RuntimeException e) {
            fail(format("Expected query to succeed: %s", sql), e);
        }
    }

    protected void assertQueryFails(String sql, String expectedMessageRegExp)
    {
        try {
            queryRunner.execute(queryRunner.getDefaultSession(), sql);
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException ex) {
            if (!nullToEmpty(ex.getMessage()).matches(expectedMessageRegExp)) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", ex.getMessage(),
                        expectedMessageRegExp, sql), ex);
            }
        }
    }
}
