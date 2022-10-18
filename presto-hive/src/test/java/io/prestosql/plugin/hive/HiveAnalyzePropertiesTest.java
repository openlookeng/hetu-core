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
package io.prestosql.plugin.hive;

import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveAnalyzePropertiesTest
{
    @Mock
    private TypeManager mockTypeManager;

    private HiveAnalyzeProperties hiveAnalyzePropertiesUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveAnalyzePropertiesUnderTest = new HiveAnalyzeProperties(FunctionAndTypeManager.createTestFunctionAndTypeManager());
    }

    @Test
    public void testGetPartitionList()
    {
        // Setup
        final Map<String, Object> properties = new HashMap<>();

        // Run the test
        final Optional<List<List<String>>> result = HiveAnalyzeProperties.getPartitionList(properties);
    }

    @Test
    public void testGetAnalyzeProperties()
    {
        List<PropertyMetadata<?>> analyzeProperties = hiveAnalyzePropertiesUnderTest.getAnalyzeProperties();
    }
}
