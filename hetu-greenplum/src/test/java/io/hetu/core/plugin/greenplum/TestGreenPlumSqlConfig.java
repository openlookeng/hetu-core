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
package io.hetu.core.plugin.greenplum;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestGreenPlumSqlConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(GreenPlumSqlConfig.class)
                .setArrayMapping(GreenPlumSqlConfig.ArrayMapping.DISABLED)
                .setAllowModifyTable(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("greenplum.experimental.array-mapping", "AS_ARRAY")
                .put("greenplum.allow-modify-table", "false")
                .build();

        GreenPlumSqlConfig expected = new GreenPlumSqlConfig()
                .setArrayMapping(GreenPlumSqlConfig.ArrayMapping.AS_ARRAY)
                .setAllowModifyTable(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
