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

package io.hetu.core.plugin.opengauss;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.prestosql.plugin.postgresql.PostgreSqlConfig;
import io.prestosql.plugin.postgresql.TestPostgreSqlConfig;
import org.testng.annotations.Test;

import java.util.Map;

public class TestOpenGaussConfig
        extends TestPostgreSqlConfig
{
    @Test
    public void testExplicitPropertyMetaDataSpeedup()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("opengauss.metadata.speedup", "true")
                .put("postgresql.experimental.array-mapping", "AS_ARRAY")
                .build();

        OpenGaussClientConfig expected = new OpenGaussClientConfig()
                .setMetaDataSpeedup(true);
        expected.setArrayMapping(PostgreSqlConfig.ArrayMapping.AS_ARRAY);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
