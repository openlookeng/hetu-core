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

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.mockito.MockitoAnnotations.initMocks;

public class HdfsConfigurationInitializerTest
{
    @Mock
    private HiveConfig mockConfig;

    private HdfsConfigurationInitializer hdfsConfigurationInitializerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hdfsConfigurationInitializerUnderTest = new HdfsConfigurationInitializer(new HiveConfig(), new HashSet<>());
    }

    @Test
    public void testNoOpDNSToSwitchMapping()
    {
        HdfsConfigurationInitializer.NoOpDNSToSwitchMapping noOpDNSToSwitchMapping = new HdfsConfigurationInitializer.NoOpDNSToSwitchMapping();
        noOpDNSToSwitchMapping.resolve(Arrays.asList("name"));
        noOpDNSToSwitchMapping.reloadCachedMappings();
    }
}
