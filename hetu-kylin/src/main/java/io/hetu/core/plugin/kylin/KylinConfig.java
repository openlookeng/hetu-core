/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.kylin;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.Mandatory;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.math.RoundingMode;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;

public class KylinConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private boolean isQueryPushDownEnabled;
    private RoundingMode roundingMode;
    private boolean enablePredicatePushdownWithOrOperator;
    private boolean cubeMetadataFilter;
    private boolean includeRowKeyMetadataFilter;
    private boolean includeMeasuresMetadataFilter;
    private boolean includeJoinKeyMetadataFilter;
    private long cubeMetadataCacheMaxSize = 10000;
    private Optional<String> restUrl = Optional.empty();
    private Duration cubeMetadataCacheTtl = new Duration(60, MINUTES);
    private String explainMatchPattern = "KapOLAPToEnumerableConverter";
    private String sqlReplacePattern = "";
    private ValidateSqlMethod validateSqlMethod = ValidateSqlMethod.AUTO;
    private boolean needBypassCastExpression;
    private boolean partialPushdownEnableWithCubePriority;
    private String connectorPlanOptimizerRuleBlackList;

    public boolean isQueryPushDownEnabled()
    {
        return isQueryPushDownEnabled;
    }

    /**
     * set Query Push Down Enabled
     *
     * @param isQueryPushDownEnabledParameter config from properties
     * @return config object
     */
    @Config("hetu.query.pushdown.enabled")
    @ConfigDescription("Enable sub-query push down to this data center. It's set by default")
    public KylinConfig setQueryPushDownEnabled(boolean isQueryPushDownEnabledParameter)
    {
        this.isQueryPushDownEnabled = isQueryPushDownEnabledParameter;
        return this;
    }

    public RoundingMode getRoundingMode()
    {
        return roundingMode;
    }

    /**
     * set Round mode
     *
     * @param roundingMode config from properties
     * @return object
     */
    @Config("number.rounding-mode")
    @ConfigDescription("Rounding mode for  NUMBER data type. "
            + "This is useful when Oracle NUMBER data type specifies higher scale than is supported in Presto")
    public KylinConfig setRoundingMode(RoundingMode roundingMode)
    {
        this.roundingMode = roundingMode;
        return this;
    }

    /**
     * Getter for 'enable-predicate-pushdown-with-or-operator' config.
     *
     * @return boolean true if OR operator pushdown is enabled otherwise fasle.
     */
    public boolean isEnablePredicatePushdownWithOrOperator()
    {
        return enablePredicatePushdownWithOrOperator;
    }

    /**
     * Setter for setting 'enable-predicate-pushdown-with-or-operator' config value.
     *
     * @param enablePredicatePushdownWithOrOperator true for enabling OR operator pushdown otherwise false.
     * @return PostgreSqlConfig
     */
    @Config("enable-predicate-pushdown-with-or-operator")
    public KylinConfig setEnablePredicatePushdownWithOrOperator(boolean enablePredicatePushdownWithOrOperator)
    {
        this.enablePredicatePushdownWithOrOperator = enablePredicatePushdownWithOrOperator;
        return this;
    }

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public KylinConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    public KylinConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public KylinConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    public Optional<String> getRestUrl()
    {
        return restUrl;
    }

    @Config("rest-api")
    @Description("Connection to rest url")
    public KylinConfig setRestUrl(String restUrl)
    {
        this.restUrl = Optional.of(restUrl);
        return this;
    }

    public boolean isCubeMetadataFilter()
    {
        return cubeMetadataFilter;
    }

    @Config("cube-metadata-filter")
    @Description("validate columns against cube")
    public KylinConfig setCubeMetadataFilter(boolean cubeMetadataFilter)
    {
        this.cubeMetadataFilter = cubeMetadataFilter;
        return this;
    }

    public boolean isIncludeMeasuresMetadataFilter()
    {
        return includeMeasuresMetadataFilter;
    }

    @Config("kylin.cube-metadata-filter-include-measures")
    @Description("Including measures in cube metadata filter")
    public KylinConfig setIncludeMeasuresMetadataFilter(boolean includeMeasuresMetadataFilter)
    {
        this.includeMeasuresMetadataFilter = includeMeasuresMetadataFilter;
        return this;
    }

    public boolean isIncludeRowKeyMetadataFilter()
    {
        return includeRowKeyMetadataFilter;
    }

    @Config("kylin.cube-metadata-filter-include-rowkey")
    @Description("Including row_key in cube metadata filter")
    public KylinConfig setIncludeRowKeyMetadataFilter(boolean includeRowKeyMetadataFilter)
    {
        this.includeRowKeyMetadataFilter = includeRowKeyMetadataFilter;
        return this;
    }

    public boolean isIncludeJoinKeyMetadataFilter()
    {
        return includeJoinKeyMetadataFilter;
    }

    @Config("kylin.cube-metadata-filter-include-joinkey")
    @Description("Including join key in cube metadata filer")
    public KylinConfig setIncludeJoinKeyMetadataFilter(boolean includeJoinKeyMetadataFilter)
    {
        this.includeJoinKeyMetadataFilter = includeJoinKeyMetadataFilter;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCubeMetadataCacheTtl()
    {
        return cubeMetadataCacheTtl;
    }

    @Config("kylin.cube-metadata-cache-ttl")
    @Description("set cube metadata cache timeout in minutes")
    public KylinConfig setCubeMetadataCacheTtl(Duration cubeMetadataCacheTtl)
    {
        this.cubeMetadataCacheTtl = cubeMetadataCacheTtl;
        return this;
    }

    @Min(1)
    public long getCubeMetadataCacheMaxSize()
    {
        return cubeMetadataCacheMaxSize;
    }

    @Config("kylin.cube-metadata-cache-max-size")
    @Description("set cube metadata cache size")
    public KylinConfig setCubeMetadataCacheMaxSize(long cubeMetadataCacheMaxSize)
    {
        this.cubeMetadataCacheMaxSize = cubeMetadataCacheMaxSize;
        return this;
    }

    public String getExplainMatchPattern()
    {
        return this.explainMatchPattern;
    }

    @Config("kylin.pushdown-explain-pattern")
    @Description("Pattern to check in explain plan")
    public KylinConfig setExplainMatchPattern(String explainMatchPattern)
    {
        this.explainMatchPattern = explainMatchPattern;
        return this;
    }

    public String getSqlReplacePattern()
    {
        return this.sqlReplacePattern;
    }

    @Config("kylin.single-catalog-sql-replace-pattern")
    @Description("Pattern to check in explain plan")
    public KylinConfig setSqlReplacePattern(String sqlReplacePattern)
    {
        this.sqlReplacePattern = sqlReplacePattern;
        return this;
    }

    public ValidateSqlMethod getValidateSqlMethod()
    {
        return this.validateSqlMethod;
    }

    @Config("kylin.validate-sql-method")
    @Description("validate sql method ")
    public KylinConfig setValidateSqlMethod(ValidateSqlMethod validateSqlMethod)
    {
        this.validateSqlMethod = validateSqlMethod;
        return this;
    }

    public boolean isNeedBypassCastExpression()
    {
        return needBypassCastExpression;
    }

    @Config("kylin.bypass-cast-for-tablescan")
    @Description("Including join key in cube metadata filer")
    public KylinConfig setNeedBypassCastExpression(boolean needByepassCastExpression)
    {
        this.needBypassCastExpression = needByepassCastExpression;
        return this;
    }

    public boolean isPartialPushdownEnableWithCubePriority()
    {
        return partialPushdownEnableWithCubePriority;
    }

    @Config("kylin.partial-pushdown-enable-with-cube-priority")
    @Description("Is partial pushdown enabled when cube priority is present")
    public KylinConfig setPartialPushdownEnableWithCubePriority(boolean partialPushdownEnableWithCubePriority)
    {
        this.partialPushdownEnableWithCubePriority = partialPushdownEnableWithCubePriority;
        return this;
    }

    public String getConnectorPlanOptimizerRuleBlackList()
    {
        return connectorPlanOptimizerRuleBlackList;
    }

    @Mandatory(name = "connector-planoptimizer-rule-blacklist",
            description = "connector planoptimizer rule blacklist",
            defaultValue = "io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy",
            required = false)
    @Config("connector-planoptimizer-rule-blacklist")
    @ConfigDescription("connector planoptimizer rule blacklist ")
    public KylinConfig setConnectorPlanOptimizerRuleBlackList(String connectorPlanOptimizerRuleBlackList)
    {
        this.connectorPlanOptimizerRuleBlackList = connectorPlanOptimizerRuleBlackList;
        return this;
    }

    public enum ValidateSqlMethod
    {
        REST,
        EXPLAIN,
        AUTO
    }
}
