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

package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestStarTreeQueries
        extends AbstractTestQueryFramework
{
    protected AbstractTestStarTreeQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void testStarTreeSessionProperty()
    {
        MaterializedResult result = computeActual("SET SESSION enable_star_tree_index = true");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("enable_star_tree_index", "true"));
        result = computeActual("SET SESSION enable_star_tree_index = false");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("enable_star_tree_index", "false"));
    }

    @Test
    public void testStarTree()
    {
        Session sessionStarTree = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "true")
                .build();
        Session sessionNoStarTree = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "false")
                .build();
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_cube ON nation " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE nation_cube where nationkey > -1", 25);
        assertQuery(sessionStarTree, "SELECT min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey");
        assertQuery(sessionStarTree, "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation group by nationkey");
        assertQuery(sessionStarTree, "SELECT avg(nationkey) from nation group by nationkey");
        assertUpdate("DROP CUBE nation_cube");
    }
}
