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
package io.prestosql.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.metadata.MetadataUtil.toCatalogSchemaTableName;
import static io.prestosql.metadata.MetadataUtil.toSchemaTableName;
import static io.prestosql.spi.StandardErrorCode.SERVER_STARTING_UP;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccessControlManager
        implements AccessControl
{
    private static final Logger log = Logger.get(AccessControlManager.class);
    private static final File ACCESS_CONTROL_CONFIGURATION = new File("etc/access-control.properties");
    private static final String ACCESS_CONTROL_PROPERTY_NAME = "access-control.name";

    private final TransactionManager transactionManager;
    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();
    private final Map<CatalogName, CatalogAccessControlEntry> connectorAccessControl = new ConcurrentHashMap<>();

    private final AtomicReference<SystemAccessControl> systemAccessControl = new AtomicReference<>(new InitializingSystemAccessControl());
    private final AtomicBoolean systemAccessControlLoading = new AtomicBoolean();

    private final CounterStat authenticationSuccess = new CounterStat();
    private final CounterStat authenticationFail = new CounterStat();
    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();

    @Inject
    public AccessControlManager(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        addSystemAccessControlFactory(new AllowAllSystemAccessControl.Factory());
        addSystemAccessControlFactory(new ReadOnlySystemAccessControl.Factory());
        addSystemAccessControlFactory(new FileBasedSystemAccessControl.Factory());
    }

    public void addSystemAccessControlFactory(SystemAccessControlFactory accessControlFactory)
    {
        requireNonNull(accessControlFactory, "accessControlFactory is null");

        if (systemAccessControlFactories.putIfAbsent(accessControlFactory.getName(), accessControlFactory) != null) {
            throw new IllegalArgumentException(format("Access control '%s' is already registered", accessControlFactory.getName()));
        }
    }

    public void addCatalogAccessControl(CatalogName catalogName, ConnectorAccessControl accessControl)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(accessControl, "accessControl is null");
        checkState(connectorAccessControl.putIfAbsent(catalogName, new CatalogAccessControlEntry(catalogName, accessControl)) == null,
                "Access control for connector '%s' is already registered", catalogName);
    }

    public void removeCatalogAccessControl(CatalogName catalogName)
    {
        connectorAccessControl.remove(catalogName);
    }

    public void loadSystemAccessControl()
            throws Exception
    {
        if (ACCESS_CONTROL_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(ACCESS_CONTROL_CONFIGURATION.getPath()));

            String accessControlName = properties.remove(ACCESS_CONTROL_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(accessControlName),
                    "Access control configuration %s does not contain %s", ACCESS_CONTROL_CONFIGURATION.getAbsoluteFile(), ACCESS_CONTROL_PROPERTY_NAME);

            setSystemAccessControl(accessControlName, properties);
        }
        else {
            setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        }
    }

    @VisibleForTesting
    protected void setSystemAccessControl(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        checkState(systemAccessControlLoading.compareAndSet(false, true), "System access control already initialized");

        log.info("-- Loading system access control --");

        SystemAccessControlFactory systemAccessControlFactory = systemAccessControlFactories.get(name);
        checkState(systemAccessControlFactory != null, "Access control %s is not registered", name);

        SystemAccessControl systemAccessControl = systemAccessControlFactory.create(ImmutableMap.copyOf(properties));
        this.systemAccessControl.set(systemAccessControl);

        log.info("-- Loaded system access control %s --", name);
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanSetUser(principal, userName));
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(userName, "userName is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanImpersonateUser(identity, userName));
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogs, "catalogs is null");

        return systemAccessControl.get().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanAccessCatalogs(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanShowCatalogs(identity));
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanAccessCatalog(identity, catalogName));
    }

    @Override
    public void checkCanCreateCatalog(Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        authenticationCheck(() -> systemAccessControl.get().checkCanCreateCatalog(identity, catalogName));
    }

    @Override
    public void checkCanDropCatalog(Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanDropCatalog(identity, catalogName));
    }

    @Override
    public void checkCanUpdateCatalog(Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanUpdateCatalog(identity, catalogName));
    }

    @Override
    public void checkCanUpdateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanUpdateTable(identity, toCatalogSchemaTableName(tableName)));
        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanUpdateTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateSchema(identity, schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateSchema(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(schemaName.getCatalogName()), schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropSchema(identity, schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropSchema(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(schemaName.getCatalogName()), schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameSchema(identity, schemaName, newSchemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameSchema(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(schemaName.getCatalogName()), schemaName.getSchemaName(), newSchemaName));
        }
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowSchemas(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowSchemas(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName)));
        }
    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        if (filterCatalogs(identity, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        schemaNames = systemAccessControl.get().filterSchemas(identity, catalogName, schemaNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            schemaNames = entry.getAccessControl().filterSchemas(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateTable(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropTable(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameTable(identity, toCatalogSchemaTableName(tableName), toCatalogSchemaTableName(newTableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName), toSchemaTableName(newTableName)));
        }
    }

    @Override
    public void checkCanSetTableComment(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSetTableComment(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetTableComment(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, CatalogSchemaName schema)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schema, "schema is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, schema.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowTablesMetadata(identity, schema));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schema.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowTablesMetadata(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(), schema.getSchemaName()));
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        if (filterCatalogs(identity, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        tableNames = systemAccessControl.get().filterTables(identity, catalogName, tableNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            tableNames = entry.getAccessControl().filterTables(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), tableNames);
        }
        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(TransactionId transactionId, Identity identity, CatalogSchemaTableName table)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(table, "table is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanShowColumnsMetadata(identity, table));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, table.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowColumnsMetadata(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(), table.getSchemaTableName()));
        }
    }

    @Override
    public List<ColumnMetadata> filterColumns(TransactionId transactionId, Identity identity, CatalogSchemaTableName table, List<ColumnMetadata> columns)
    {
        requireNonNull(transactionId, "transaction is null");
        requireNonNull(identity, "identity is null");
        requireNonNull(table, "tableName is null");

        if (filterTables(transactionId, identity, table.getCatalogName(), ImmutableSet.of(table.getSchemaTableName())).isEmpty()) {
            return ImmutableList.of();
        }

        columns = systemAccessControl.get().filterColumns(identity, table, columns);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, table.getCatalogName());
        if (entry != null) {
            columns = entry.getAccessControl().filterColumns(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(), table.getSchemaTableName(), columns);
        }
        return columns;
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanAddColumn(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanAddColumn(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropColumn(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropColumn(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDropPartition(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropPartition(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropPartition(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameColumn(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameColumn(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanInsertIntoTable(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanInsertIntoTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDeleteFromTable(identity, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDeleteFromTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanCreateIndex(TransactionId transactionId, Identity identity, QualifiedObjectName indexName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(indexName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, indexName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateIndex(identity, toCatalogSchemaTableName(indexName)));
        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, indexName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateIndex(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(indexName.getCatalogName()), toSchemaTableName(indexName)));
        }
    }

    @Override
    public void checkCanDropIndex(TransactionId transactionId, Identity identity, QualifiedObjectName indexName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(indexName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, indexName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropIndex(identity, toCatalogSchemaTableName(indexName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, indexName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropIndex(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(indexName.getCatalogName()), toSchemaTableName(indexName)));
        }
    }

    @Override
    public void checkCanRenameIndex(TransactionId transactionId, Identity identity, QualifiedObjectName indexName, QualifiedObjectName newIndexName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(indexName, "indexName is null");
        requireNonNull(newIndexName, "newIndexName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, indexName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameIndex(identity, toCatalogSchemaTableName(indexName), toCatalogSchemaTableName(newIndexName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, indexName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameIndex(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(indexName.getCatalogName()), toSchemaTableName(indexName), toSchemaTableName(newIndexName)));
        }
    }

    @Override
    public void checkCanUpdateIndex(TransactionId transactionId, Identity identity, QualifiedObjectName indexName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(indexName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanUpdateIndex(identity, toCatalogSchemaTableName(indexName)));
        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, indexName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanUpdateTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(indexName.getCatalogName()), toSchemaTableName(indexName)));
        }
    }

    @Override
    public void checkCanShowIndex(TransactionId transactionId, Identity identity, QualifiedObjectName indexName)
    {
        requireNonNull(identity, "identity is null");

        if (indexName == null) {
            authenticationCheck(() -> systemAccessControl.get().checkCanShowIndex(identity, null));
            return;
        }

        authenticationCheck(() -> checkCanAccessCatalog(identity, indexName.getCatalogName()));

        authenticationCheck(() -> systemAccessControl.get().checkCanShowIndex(identity, toCatalogSchemaTableName(indexName)));
        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, indexName.getCatalogName());
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowIndex(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(indexName.getCatalogName()), toSchemaTableName(indexName)));
        }
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateView(identity, toCatalogSchemaTableName(viewName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateView(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(viewName.getCatalogName()), toSchemaTableName(viewName)));
        }
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropView(identity, toCatalogSchemaTableName(viewName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropView(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(viewName.getCatalogName()), toSchemaTableName(viewName)));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromColumns(identity, toCatalogSchemaTableName(tableName), columnNames));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromColumns(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName), columnNames));
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee,
            boolean withGrantOption)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanGrantTablePrivilege(identity, privilege, toCatalogSchemaTableName(tableName), grantee, withGrantOption));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantTablePrivilege(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), privilege, toSchemaTableName(tableName), grantee, withGrantOption));
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee,
            boolean grantOptionFor)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRevokeTablePrivilege(identity, privilege, toCatalogSchemaTableName(tableName), revokee, grantOptionFor));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeTablePrivilege(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), privilege, toSchemaTableName(tableName), revokee, grantOptionFor));
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSetSystemSessionProperty(identity, propertyName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanSetCatalogSessionProperty(identity, catalogName, propertyName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetCatalogSessionProperty(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), propertyName));
        }
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromColumns(identity, toCatalogSchemaTableName(tableName), columnNames));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromColumns(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), toSchemaTableName(tableName), columnNames));
        }
    }

    @Override
    public void checkCanCreateRole(TransactionId transactionId, Identity identity, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateRole(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), role, grantor));
        }
    }

    @Override
    public void checkCanDropRole(TransactionId transactionId, Identity identity, String role, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropRole(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), role));
        }
    }

    @Override
    public void checkCanGrantRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<
            PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), roles, grantees, withAdminOption, grantor, catalogName));
        }
    }

    @Override
    public void checkCanRevokeRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<
            PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), roles, grantees, adminOptionFor, grantor, catalogName));
        }
    }

    @Override
    public void checkCanSetRole(TransactionId transactionId, Identity identity, String role, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetRole(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), role, catalogName));
        }
    }

    @Override
    public void checkCanShowRoles(TransactionId transactionId, Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), catalogName));
        }
    }

    @Override
    public void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowCurrentRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), catalogName));
        }
    }

    @Override
    public void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowRoleGrants(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), catalogName));
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(transactionId, "transactionId is null");
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "table name is null");

        ImmutableList.Builder<ViewExpression> filters = ImmutableList.builder();

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            entry.getAccessControl().getRowFilter(entry.getTransactionHandle(transactionId), identity, toSchemaTableName(tableName)).ifPresent(filters::add);
        }

        systemAccessControl.get().getRowFilter(identity, toCatalogSchemaTableName(tableName)).ifPresent(filters::add);

        return filters.build();
    }

    @Override
    public List<ViewExpression> getColumnMasks(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, String columnName, Type type)
    {
        requireNonNull(transactionId, "securityContext is null");
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "table name is null");

        ImmutableList.Builder<ViewExpression> filters = ImmutableList.builder();

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            entry.getAccessControl().getColumnMask(entry.getTransactionHandle(transactionId), identity, toSchemaTableName(tableName), columnName, type).ifPresent(filters::add);
        }

        systemAccessControl.get().getColumnMask(identity, toCatalogSchemaTableName(tableName), columnName, type).ifPresent(filters::add);

        return filters.build();
    }

    @Override
    public void checkCanAccessNodeInfo(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanAccessNodeInfo(identity));
    }

    private CatalogAccessControlEntry getConnectorAccessControl(TransactionId transactionId, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                .map(metadata -> connectorAccessControl.get(metadata.getCatalogName()))
                .orElse(null);
    }

    @Managed
    @Nested
    public CounterStat getAuthenticationSuccess()
    {
        return authenticationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthenticationFail()
    {
        return authenticationFail;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationSuccess()
    {
        return authorizationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationFail()
    {
        return authorizationFail;
    }

    private void authenticationCheck(Runnable runnable)
    {
        try {
            runnable.run();
            authenticationSuccess.update(1);
        }
        catch (PrestoException e) {
            authenticationFail.update(1);
            throw e;
        }
    }

    private void authorizationCheck(Runnable runnable)
    {
        try {
            runnable.run();
            authorizationSuccess.update(1);
        }
        catch (PrestoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private static class InitializingSystemAccessControl
            implements SystemAccessControl
    {
        @Override
        public void checkCanSetUser(Optional<Principal> principal, String userName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanImpersonateUser(Identity identity, String userName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanAccessCatalog(Identity identity, String catalogName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }

    private class CatalogAccessControlEntry
    {
        private final CatalogName catalogName;
        private final ConnectorAccessControl accessControl;

        public CatalogAccessControlEntry(CatalogName catalogName, ConnectorAccessControl accessControl)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public ConnectorAccessControl getAccessControl()
        {
            return accessControl;
        }

        public ConnectorTransactionHandle getTransactionHandle(TransactionId transactionId)
        {
            return transactionManager.getConnectorTransaction(transactionId, catalogName);
        }
    }
}
