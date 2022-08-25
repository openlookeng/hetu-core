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
package io.hetu.core.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;

public class TestIcebergConnectorFactory
{
    private IcebergConnectorFactory icebergConnectorFactory;

    @Test
    public void testBasicConfig()
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://192.168.31.120:9000")
                .build();
        createConnector(config);
    }

    @Test
    public void test()
    {
        icebergConnectorFactory.getName();
        icebergConnectorFactory.getHandleResolver();
    }

    @Test
    public void testCachingHiveMetastore()
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://192.168.31.120:9000")
                .put("hive.metastore-cache-ttl", "5m")
                .build();
        createConnector(config);
    }

    private static void createConnector(Map<String, String> config)
    {
        ConnectorFactory factory = new IcebergConnectorFactory();
        factory.create("test", config, new TestingConnectorContext());
    }
}
