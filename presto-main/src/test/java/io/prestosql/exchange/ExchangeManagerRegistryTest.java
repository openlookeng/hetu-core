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
package io.prestosql.exchange;

import io.airlift.log.Logger;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.exchange.ExchangeHandleResolver;
import io.prestosql.spi.exchange.ExchangeManager;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static org.testng.Assert.assertNotNull;

public class ExchangeManagerRegistryTest
{
    private static final Logger log = Logger.get(ExchangeManagerRegistryTest.class);

    private static final File CONFIG_FILE = new File("etc/exchange-manager.properties");

    @Test
    public void testLoadExchangeManager() throws IOException
    {
        Map<String, String> properties = loadPropertiesFrom(CONFIG_FILE.getPath());
        try (TestingPrestoServer server = new TestingPrestoServer(properties)) {
            ExchangeManagerRegistry exchangeManagerRegistry = new ExchangeManagerRegistry(new ExchangeHandleResolver());
            exchangeManagerRegistry.loadExchangeManager(new FileSystemClientManager());
            ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
            assertNotNull(exchangeManager);
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
