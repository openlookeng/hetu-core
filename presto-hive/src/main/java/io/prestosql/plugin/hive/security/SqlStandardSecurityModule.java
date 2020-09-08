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
package io.prestosql.plugin.hive.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorAccessControl;

import static io.prestosql.plugin.hive.security.SecurityConstants.WHITE_LIST_SQLSTANDARDACCESSCONTROL_IMPL;

public class SqlStandardSecurityModule
        implements Module
{
    private static final Logger log = Logger.get(SqlStandardSecurityModule.class);

    private String sqlStandardAccessControlImp;

    public SqlStandardSecurityModule(String sqlStandardAccessControlImp)
    {
        this.sqlStandardAccessControlImp = sqlStandardAccessControlImp;
    }

    @Override
    public void configure(Binder binder)
    {
        if (sqlStandardAccessControlImp.isEmpty()) {
            binder.bind(ConnectorAccessControl.class).to(SqlStandardAccessControl.class).in(Scopes.SINGLETON);
        }
        else {
            try {
                if (!WHITE_LIST_SQLSTANDARDACCESSCONTROL_IMPL.contains(sqlStandardAccessControlImp)) {
                    throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND, "Found illegal class when binding ConnectorAccessControl.");
                }
                log.info("Binding ConnectorAccessControl.class to %s", sqlStandardAccessControlImp);
                binder.bind(ConnectorAccessControl.class)
                        .to((Class<? extends ConnectorAccessControl>) Class.forName(this.sqlStandardAccessControlImp))
                        .in(Scopes.SINGLETON);
            }
            catch (ClassNotFoundException e) {
                log.error("Failed to bind ConnectorAccessControl to a specified class. Error: %s", e.getLocalizedMessage());
                throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND, "Class not found when binding ConnectorAccessControl.");
            }
        }
        binder.bind(AccessControlMetadataFactory.class).to(SqlStandardAccessControlMetadataFactory.class);
    }

    private static final class SqlStandardAccessControlMetadataFactory
            implements AccessControlMetadataFactory
    {
        public SqlStandardAccessControlMetadataFactory() {}

        @Override
        public AccessControlMetadata create(SemiTransactionalHiveMetastore metastore)
        {
            return new SqlStandardAccessControlMetadata(metastore);
        }
    }
}
