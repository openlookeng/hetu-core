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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.hetu.core.plugin.exchange.filesystem.hdfs.HdfsFileSystemExchangeStorage;
import io.hetu.core.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import io.prestosql.spi.PrestoException;

import java.net.URI;
import java.util.List;

import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FileSystemExchangeModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FileSystemExchangeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileSystemExchangeStats.class).withGeneratedName();

        List<URI> baseDirectories = buildConfigObject(FileSystemExchangeConfig.class).getBaseDirectories();
        if (baseDirectories.stream().map(URI::getScheme).distinct().count() != 1) {
            throw new PrestoException(CONFIGURATION_INVALID, "Multiple schemes in exchange base directories");
        }
        String scheme = baseDirectories.get(0).getScheme();
        if (scheme == null || scheme.equals("file")) {
            binder.bind(FileSystemExchangeStorage.class).to(LocalFileSystemExchangeStorage.class).in(Scopes.SINGLETON);
        }
        else if (ImmutableSet.of("hdfs").contains(scheme)) {
            binder.bind(FileSystemExchangeStorage.class).to(HdfsFileSystemExchangeStorage.class).in(Scopes.SINGLETON);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Scheme %s is not supported as exchange storage", scheme));
        }
        binder.bind(FileSystemExchangeManager.class).in(Scopes.SINGLETON);
    }
}
