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
package io.prestosql.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.function.Mandatory;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.parserExternalFunctionCatalogSchema;
import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.parserPushDownSupportedRemoteCatalogSchema;
import static io.prestosql.sql.builder.functioncall.FunctionCallConstants.REMOTE_FUNCTION_CATALOG_SCHEMA;
import static java.util.concurrent.TimeUnit.MINUTES;

public class BaseJdbcConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String userCredentialName;
    private String passwordCredentialName;
    private boolean caseInsensitiveNameMatching;
    private boolean caseInsensitiveNameMatchingConfigured;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private boolean useConnectionPool;
    private int maxTotal = 50;
    private int maxIdle = 30;
    private int minIdle = 10;
    private boolean lifo = true;
    private boolean fairness;
    private long maxWaitMillis = -1L;
    private long softMinEvictableIdleTimeMillis = 1000L * 60L * 30L;
    private int numTestsPerEvictionRun = 3;
    private boolean testOnCreate;
    private boolean testOnBorrow = true;
    private boolean testOnReturn;
    private boolean testWhileIdle = true;
    private long timeBetweenEvictionRunsMillis = -1L;
    private boolean blockWhenExhausted = true;
    private boolean jmxEnabled = true;
    // Hetu: JDBC fetch size configuration
    private int fetchSize;
    private boolean dmlStatementsCommitInATransaction;
    // Hetu: JDBC query push down enable
    private boolean pushDownEnable = true;
    // Hetu: JDBC push down module
    private JdbcPushDownModule pushDownModule = JdbcPushDownModule.DEFAULT;

    private String pushDownExternalFunctionNamespaces;
    private String connectorRegistryFunctionNamespace;

    private boolean fieldSplitEnable;

    private String tableSplitFields;

    private Duration stepCalcRefreshInterval = new Duration(5, TimeUnit.MINUTES);
    private int stepCalcThreads = 4;

    public Optional<CatalogSchemaName> getConnectorRegistryFunctionNamespace()
    {
        return parserExternalFunctionCatalogSchema(connectorRegistryFunctionNamespace);
    }

    @Config("connector.externalfunction.namespace")
    public BaseJdbcConfig setConnectorRegistryFunctionNamespace(String connectorRegistryFunctionNamespace)
    {
        this.connectorRegistryFunctionNamespace = connectorRegistryFunctionNamespace;
        return this;
    }

    @Nullable
    public List<CatalogSchemaName> getPushDownExternalFunctionNamespace()
    {
        return parserPushDownSupportedRemoteCatalogSchema(this.pushDownExternalFunctionNamespaces);
    }

    @Config(REMOTE_FUNCTION_CATALOG_SCHEMA)
    @ConfigDescription("The namespace in which remote function can push down, written in a specific catalog.schema, for example: mem.mysql," +
            " or in multiple specific catalog.schema, for example: mem.mysql|fs.mysql|metaCache.mysql")
    public BaseJdbcConfig setPushDownExternalFunctionNamespace(String pushDownExternalFunctionNamespaceStr)
    {
        this.pushDownExternalFunctionNamespaces = pushDownExternalFunctionNamespaceStr;
        return this;
    }

    public boolean isLifo()
    {
        return lifo;
    }

    @Config("jdbc.connection.pool.lifo")
    public BaseJdbcConfig setLifo(boolean lifo)
    {
        this.lifo = lifo;
        return this;
    }

    public boolean isFairness()
    {
        return fairness;
    }

    @Config("jdbc.connection.pool.fairness")
    public BaseJdbcConfig setFairness(boolean fairness)
    {
        this.fairness = fairness;
        return this;
    }

    public long getMaxWaitMillis()
    {
        return maxWaitMillis;
    }

    @Config("jdbc.connection.pool.maxWaitMillis")
    public BaseJdbcConfig setMaxWaitMillis(long maxWaitMillis)
    {
        this.maxWaitMillis = maxWaitMillis;
        return this;
    }

    public long getSoftMinEvictableIdleTimeMillis()
    {
        return softMinEvictableIdleTimeMillis;
    }

    @Config("jdbc.connection.pool.softMinEvictableIdleTimeMillis")
    public BaseJdbcConfig setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis)
    {
        this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
        return this;
    }

    public int getNumTestsPerEvictionRun()
    {
        return numTestsPerEvictionRun;
    }

    @Config("jdbc.connection.pool.numTestsPerEvictionRun")
    public BaseJdbcConfig setNumTestsPerEvictionRun(int numTestsPerEvictionRun)
    {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
        return this;
    }

    public boolean isTestOnCreate()
    {
        return testOnCreate;
    }

    @Config("jdbc.connection.pool.testOnCreate")
    public BaseJdbcConfig setTestOnCreate(boolean testOnCreate)
    {
        this.testOnCreate = testOnCreate;
        return this;
    }

    public boolean isTestOnBorrow()
    {
        return testOnBorrow;
    }

    @Config("jdbc.connection.pool.testOnBorrow")
    public BaseJdbcConfig setTestOnBorrow(boolean testOnBorrow)
    {
        this.testOnBorrow = testOnBorrow;
        return this;
    }

    public boolean isTestOnReturn()
    {
        return testOnReturn;
    }

    @Config("jdbc.connection.pool.testOnReturn")
    public BaseJdbcConfig setTestOnReturn(boolean testOnReturn)
    {
        this.testOnReturn = testOnReturn;
        return this;
    }

    public boolean isTestWhileIdle()
    {
        return testWhileIdle;
    }

    @Config("jdbc.connection.pool.testWhileIdle")
    public BaseJdbcConfig setTestWhileIdle(boolean testWhileIdle)
    {
        this.testWhileIdle = testWhileIdle;
        return this;
    }

    public long getTimeBetweenEvictionRunsMillis()
    {
        return timeBetweenEvictionRunsMillis;
    }

    @Config("jdbc.connection.pool.timeBetweenEvictionRunsMillis")
    public BaseJdbcConfig setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis)
    {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        return this;
    }

    public boolean isBlockWhenExhausted()
    {
        return blockWhenExhausted;
    }

    @Config("jdbc.connection.pool.blockWhenExhausted")
    public BaseJdbcConfig setBlockWhenExhausted(boolean blockWhenExhausted)
    {
        this.blockWhenExhausted = blockWhenExhausted;
        return this;
    }

    public boolean isJmxEnabled()
    {
        return jmxEnabled;
    }

    @Config("jdbc.connection.pool.jmxEnabled")
    public BaseJdbcConfig setJmxEnabled(boolean jmxEnabled)
    {
        this.jmxEnabled = jmxEnabled;
        return this;
    }

    @Nullable
    public int getMaxTotal()
    {
        return maxTotal;
    }

    @Config("jdbc.connection.pool.maxTotal")
    public BaseJdbcConfig setMaxTotal(int maxTotal)
    {
        this.maxTotal = maxTotal;
        return this;
    }

    @Nullable
    public int getMaxIdle()
    {
        return maxIdle;
    }

    @Config("jdbc.connection.pool.maxIdle")
    public BaseJdbcConfig setMaxIdle(int maxIdle)
    {
        this.maxIdle = maxIdle;
        return this;
    }

    @Nullable
    public int getMinIdle()
    {
        return minIdle;
    }

    @Config("jdbc.connection.pool.minIdle")
    public BaseJdbcConfig setMinIdle(int minIdle)
    {
        this.minIdle = minIdle;
        return this;
    }

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public boolean isUseConnectionPool()
    {
        return useConnectionPool;
    }

    @Config("use-connection-pool")
    public BaseJdbcConfig setUseConnectionPool(boolean useConnectionPool)
    {
        this.useConnectionPool = useConnectionPool;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Mandatory(name = "connection-user",
            description = "User to connect to remote database",
            defaultValue = "root",
            required = true)
    @Config("connection-user")
    public BaseJdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Mandatory(name = "connection-password",
            description = "Password of user to connect to remote database",
            defaultValue = "secret",
            required = true)
    @Config("connection-password")
    @ConfigSecuritySensitive
    public BaseJdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseJdbcConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseJdbcConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("case-insensitive-name-matching")
    public BaseJdbcConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        setCaseInsensitiveNameMatchingConfigured(true);
        return this;
    }

    public BaseJdbcConfig internalsetCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        if (!isCaseInsensitiveNameMatchingConfigured()) {
            this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        }
        return this;
    }

    public BaseJdbcConfig setCaseInsensitiveNameMatchingConfigured(boolean isConfigured)
    {
        this.caseInsensitiveNameMatchingConfigured = isConfigured;
        return this;
    }

    public boolean isCaseInsensitiveNameMatchingConfigured()
    {
        return this.caseInsensitiveNameMatchingConfigured;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public BaseJdbcConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    /**
     * Hetu allows JDBC connectors to set fetch size.
     *
     * @return the fetch size
     */
    @Min(0)
    public int getFetchSize()
    {
        return this.fetchSize;
    }

    /**
     * Hetu allows JDBC connectors to set fetch size. Assigning 0 will leave the default JDBC connection fetch size.
     *
     * @param fetchSize the fetch size
     * @return the BaseJdbcConfig
     */
    @Config("fetch-size")
    @ConfigDescription("Customize JDBC fetch size. Assigning 0 will leave the default JDBC connection fetch size.")
    public BaseJdbcConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * If a connection, all its DML statements, such as Insert, Update or Delete, will be executed and committed as a transaction. Default is false, every 1000 statements will be executed and committed as a transaction.
     */
    @Config("dml-statements-commit-in-a-transaction")
    @ConfigDescription("If a connection, all its DML statements, such as Insert, Update or Delete, will be executed and committed as a transaction. Default is false, every 1000 statements will be executed and committed as a transaction.")
    public BaseJdbcConfig setDmlStatementsCommitInATransaction(boolean dmlStatementsCommitInATransaction)
    {
        this.dmlStatementsCommitInATransaction = dmlStatementsCommitInATransaction;
        return this;
    }

    public boolean isDmlStatementsCommitInATransaction()
    {
        return dmlStatementsCommitInATransaction;
    }

    public boolean isPushDownEnable()
    {
        return pushDownEnable;
    }

    @Config("jdbc.pushdown-enabled")
    @ConfigDescription("Allow jdbc pushDown")
    public BaseJdbcConfig setPushDownEnable(boolean pushDownEnable)
    {
        this.pushDownEnable = pushDownEnable;
        return this;
    }

    public JdbcPushDownModule getPushDownModule()
    {
        return this.pushDownModule;
    }

    @Config("jdbc.pushdown-module")
    @ConfigDescription("jdbc query push down module in [FULL_PUSHDOWN, BASE_PUSHDOWN]")
    public BaseJdbcConfig setPushDownModule(JdbcPushDownModule pushDownModule)
    {
        this.pushDownModule = pushDownModule;
        return this;
    }

    @Config("jdbc.table-split-enabled")
    @ConfigDescription("table spilt function switch")
    public BaseJdbcConfig setTableSplitEnable(boolean fieldSplitEnable)
    {
        this.fieldSplitEnable = fieldSplitEnable;
        return this;
    }

    public boolean getTableSplitEnable()
    {
        return this.fieldSplitEnable;
    }

    //jdbc.table-split-fields =[{"catalogName":"mysqldb","schemaName":"","tableName":"hetu_data","splitField":"id","calcStepEnable":"false","dataReadOnly":"false","scanNodes":"2","fieldMinValue":"-1","fieldMaxValue":"3"},{"catalogName":"mysqldb","schemaName":"","tableName":"hetu_catalog_params","splitField":"catalog_id","calcStepEnable":"false","dataReadOnly":"false","scanNodes":"2","fieldMinValue":"-1","fieldMaxValue":"3"}]
    @Config("jdbc.table-split-fields")
    @ConfigDescription("Allow multi split by one of table filed, this filed must be integer like value")
    public BaseJdbcConfig setTableSplitFields(String tableSplitFields)
    {
        this.tableSplitFields = tableSplitFields;
        return this;
    }

    public String getTableSplitFields()
    {
        return this.tableSplitFields;
    }

    @Config("jdbc.table-split-stepCalc-refresh-interval")
    public BaseJdbcConfig setTableSplitStepCalcRefreshInterval(Duration stepCalcRefreshInterval)
    {
        this.stepCalcRefreshInterval = stepCalcRefreshInterval;
        return this;
    }

    public Duration getTableSplitStepCalcRefreshInterval()
    {
        return stepCalcRefreshInterval;
    }

    @Config("jdbc.table-split-stepCalc-threads")
    public BaseJdbcConfig setTableSplitStepCalcCalcThreads(int stepCalcThreads)
    {
        this.stepCalcThreads = stepCalcThreads;
        return this;
    }

    public int getTableSplitStepCalcCalcThreads()
    {
        return stepCalcThreads;
    }
}
