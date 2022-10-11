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
package io.prestosql.spi.connector;

import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ConnectorTablePropertiesTest
{
    private ConnectorTableProperties connectorTablePropertiesUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        connectorTablePropertiesUnderTest = new ConnectorTableProperties(TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())), Optional.of(new HashSet<>()),
                Optional.of(new DiscretePredicates(Arrays.asList(),
                        Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                Arrays.asList());
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, connectorTablePropertiesUnderTest.hashCode());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(connectorTablePropertiesUnderTest.equals("obj"));
    }
}
