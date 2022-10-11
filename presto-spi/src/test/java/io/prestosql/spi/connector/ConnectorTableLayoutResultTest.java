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
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ConnectorTableLayoutResultTest
{
    @Mock
    private ConnectorTableLayout mockLayout;

    private ConnectorTableLayoutResult connectorTableLayoutResultUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        connectorTableLayoutResultUnderTest = new ConnectorTableLayoutResult(mockLayout,
                TupleDomain.withColumnDomains(new HashMap<>()));
    }

    @Test
    public void testGetTableLayout() throws Exception
    {
        // Setup
        final ConnectorTableLayout expectedResult = new ConnectorTableLayout(null, Optional.of(Arrays.asList()),
                TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())), Optional.of(new HashSet<>()),
                Optional.of(new DiscretePredicates(
                        Arrays.asList(), Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                Arrays.asList());

        // Run the test
        final ConnectorTableLayout result = connectorTableLayoutResultUnderTest.getTableLayout();

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
