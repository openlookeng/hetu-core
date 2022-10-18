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
package io.prestosql.plugin.hive.metastore.glue;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastoreFactory;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.security.ConnectorIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final FileHiveMetastore metastore;

    // Glue metastore does not support impersonation, so just use single shared instance
    @Inject
    public FileHiveMetastoreFactory(FileHiveMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Flatten
    @Managed
    public FileHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return metastore;
    }
}
