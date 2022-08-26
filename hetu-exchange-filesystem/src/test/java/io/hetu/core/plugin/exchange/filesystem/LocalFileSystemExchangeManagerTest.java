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
package io.hetu.core.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableMap;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.spi.exchange.ExchangeManager;

import java.nio.file.Paths;
import java.util.Properties;

public class LocalFileSystemExchangeManagerTest
        extends AbstractTestExchangeManager
{
    @Override
    protected ExchangeManager createExchangeManager()
    {
        String baseDirectory1 = System.getProperty("java.io.tmpdir") + "/local-file-system-exchange-manager-1";
        String baseDirectory2 = System.getProperty("java.io.tmpdir") + "/local-file-system-exchange-manager-2";
        return new FileSystemExchangeManagerFactory().create(ImmutableMap.of(
                "exchange.base-directories", baseDirectory1 + "," + baseDirectory2,
                "exchange.sink-max-file-size", "32MB",
                "exchange.max-page-storage-size", "32MB"
        ), new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/")));
    }
}
