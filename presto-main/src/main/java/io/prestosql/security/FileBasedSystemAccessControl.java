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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.security.ForwardingSystemAccessControl;
import io.prestosql.plugin.base.security.ImpersonationRule;
import io.prestosql.security.IndexAccessControlRule.IndexPrivilege;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.prestosql.plugin.base.JsonUtils.parseJson;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.prestosql.security.IndexAccessControlRule.IndexPrivilege.ALL;
import static io.prestosql.security.IndexAccessControlRule.IndexPrivilege.CREATE;
import static io.prestosql.security.IndexAccessControlRule.IndexPrivilege.DROP;
import static io.prestosql.security.IndexAccessControlRule.IndexPrivilege.RENAME;
import static io.prestosql.security.IndexAccessControlRule.IndexPrivilege.SHOW;
import static io.prestosql.security.IndexAccessControlRule.IndexPrivilege.UPDATE;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.prestosql.spi.security.AccessDeniedException.denyAccessNodeInfo;
import static io.prestosql.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateIndex;
import static io.prestosql.spi.security.AccessDeniedException.denyDropIndex;
import static io.prestosql.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameIndex;
import static io.prestosql.spi.security.AccessDeniedException.denySetUser;
import static io.prestosql.spi.security.AccessDeniedException.denyShowIndex;
import static io.prestosql.spi.security.AccessDeniedException.denyUpdateIndex;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedSystemAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "file";
    private static final Logger log = Logger.get(FileBasedSystemAccessControl.class);
    private static final Pattern ANY = Pattern.compile(".*");
    private static final Pattern SYSTEM = Pattern.compile("system");
    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;
    private final List<NodeInformationRule> nodeInfoRules;
    private final List<IndexAccessControlRule> indexRules;
    private final Optional<List<ImpersonationRule>> impersonationRules;

    private FileBasedSystemAccessControl(List<CatalogAccessControlRule> catalogRules,
            Optional<List<PrincipalUserMatchRule>> principalUserMatchRules,
            List<NodeInformationRule> nodeInfoRules,
            List<IndexAccessControlRule> indexRules,
            Optional<List<ImpersonationRule>> impersonationRules)
    {
        this.catalogRules = catalogRules;
        this.principalUserMatchRules = principalUserMatchRules;
        this.nodeInfoRules = nodeInfoRules;
        this.indexRules = indexRules;
        this.impersonationRules = impersonationRules;
    }

    @Override
    public void checkCanShowCatalogs(Identity identity)
    {
        if (!canAccessCatalog(identity)) {
            denyCatalogAccess();
        }
    }

    @Override
    public void checkCanCreateCatalog(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, Optional.of(catalogName))) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanDropCatalog(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, Optional.of(catalogName))) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanUpdateCatalog(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, Optional.of(catalogName))) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanUpdateTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateIndex(Identity identity, CatalogSchemaTableName index)
    {
        if (!checkIndexPermission(identity, CREATE) && !checkIndexPermission(identity, ALL)) {
            denyCreateIndex();
        }
    }

    @Override
    public void checkCanDropIndex(Identity identity, CatalogSchemaTableName index)
    {
        if (!checkIndexPermission(identity, DROP) && !checkIndexPermission(identity, ALL)) {
            denyDropIndex();
        }
    }

    @Override
    public void checkCanRenameIndex(Identity identity, CatalogSchemaTableName index, CatalogSchemaTableName indexNew)
    {
        if (!checkIndexPermission(identity, RENAME) && !checkIndexPermission(identity, ALL)) {
            denyRenameIndex();
        }
    }

    @Override
    public void checkCanUpdateIndex(Identity identity, CatalogSchemaTableName index)
    {
        if (!checkIndexPermission(identity, UPDATE) && !checkIndexPermission(identity, ALL)) {
            denyUpdateIndex();
        }
    }

    @Override
    public void checkCanShowIndex(Identity identity, CatalogSchemaTableName index)
    {
        if (!checkIndexPermission(identity, SHOW) && !checkIndexPermission(identity, ALL)) {
            denyShowIndex();
        }
    }

    private boolean checkIndexPermission(Identity identity, IndexPrivilege... privileges)
    {
        for (IndexAccessControlRule rule : indexRules) {
            Optional<Set<IndexPrivilege>> indexPrivileges = rule.match(identity.getUser());
            if (indexPrivileges.isPresent()) {
                return indexPrivileges.get().containsAll(ImmutableSet.copyOf(privileges));
            }
        }
        return false;
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        if (!principalUserMatchRules.isPresent()) {
            return;
        }

        if (!principal.isPresent()) {
            denySetUser(principal, userName);
        }

        String principalName = principal.get().getName();

        for (PrincipalUserMatchRule rule : principalUserMatchRules.get()) {
            Optional<Boolean> allowed = rule.match(principalName, userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denySetUser(principal, userName);
            }
        }

        denySetUser(principal, userName);
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        if (!impersonationRules.isPresent()) {
            // if there are principal user match rules, we assume that impersonation checks are
            // handled there; otherwise, impersonation must be manually configured
            if (!principalUserMatchRules.isPresent()) {
                denyImpersonateUser(identity.getUser(), userName);
            }
            return;
        }

        for (ImpersonationRule rule : impersonationRules.get()) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denyImpersonateUser(identity.getUser(), userName);
            }
        }

        denyImpersonateUser(identity.getUser(), userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, Optional.of(catalogName))) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(identity, Optional.of(catalog))) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    private boolean canAccessCatalog(Identity identity)
    {
        return canAccessCatalog(identity, Optional.empty());
    }

    private boolean canAccessCatalog(Identity identity, Optional<String> catalogName)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), identity.getGroups(), catalogName);
            if (allowed.isPresent()) {
                return allowed.get();
            }
        }
        return false;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(identity, Optional.of(catalogName))) {
            return ImmutableSet.of();
        }

        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
    }

    @Override
    public void checkCanSetTableComment(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(identity, Optional.of(catalogName))) {
            return ImmutableSet.of();
        }

        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public List<ColumnMetadata> filterColumns(Identity identity, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        if (!canAccessCatalog(identity, Optional.of(tableName.getCatalogName()))) {
            return ImmutableList.of();
        }

        return columns;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanShowRoles(Identity identity, String catalogName)
    {
    }

    @Override
    public Optional<ViewExpression> getRowFilter(Identity identity, CatalogSchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(Identity identity, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    @Override
    public void checkCanAccessNodeInfo(Identity identity)
    {
        if (!canAccessNodeInfo(identity)) {
            denyAccessNodeInfo();
        }
    }

    private boolean canAccessNodeInfo(Identity identity)
    {
        for (NodeInformationRule rule : nodeInfoRules) {
            Optional<Boolean> owner = rule.match(identity.getUser());
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            String configFileName = config.get(SECURITY_CONFIG_FILE);
            checkState(configFileName != null, "Security configuration must contain the '%s' property", SECURITY_CONFIG_FILE);

            if (config.containsKey(SECURITY_REFRESH_PERIOD)) {
                Duration refreshPeriod;
                try {
                    refreshPeriod = Duration.valueOf(config.get(SECURITY_REFRESH_PERIOD));
                }
                catch (IllegalArgumentException e) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                if (refreshPeriod.toMillis() == 0) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                return ForwardingSystemAccessControl.of(memoizeWithExpiration(
                        () -> {
                            log.info("Refreshing system access control from %s", configFileName);
                            return create(configFileName);
                        },
                        refreshPeriod.toMillis(),
                        MILLISECONDS));
            }
            return create(configFileName);
        }

        private PrestoException invalidRefreshPeriodException(Map<String, String> config, String configFileName)
        {
            return new PrestoException(
                    CONFIGURATION_INVALID,
                    format("Invalid duration value '%s' for property '%s' in '%s'", config.get(SECURITY_REFRESH_PERIOD), SECURITY_REFRESH_PERIOD, configFileName));
        }

        private SystemAccessControl create(String configFileName)
        {
            FileBasedSystemAccessControlRules rules = parseJson(Paths.get(configFileName), FileBasedSystemAccessControlRules.class);

            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules());

            // Hack to allow Presto Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Presto Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    true,
                    Optional.of(ANY),
                    Optional.empty(),
                    Optional.of(SYSTEM)));

            return new FileBasedSystemAccessControl(catalogRulesBuilder.build(), rules.getPrincipalUserMatchRules(), rules.getNodeInfoRules(), rules.getIndexRules(), rules.getImpersonationRules());
        }
    }
}
