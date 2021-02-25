/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.datacenter.optimization;

import io.hetu.core.plugin.datacenter.DataCenterColumnHandle;
import io.hetu.core.plugin.datacenter.DataCenterConfig;
import io.hetu.core.plugin.datacenter.DataCenterTableHandle;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcQueryGenerator;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcSqlStatementWriter;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.LinkedHashMap;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_QUERY_GENERATOR_FAILURE;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.quote;

public class DataCenterQueryGenerator
        extends BaseJdbcQueryGenerator
{
    @Inject
    public DataCenterQueryGenerator(DataCenterConfig config, RowExpressionService rowExpressionService)
    {
        super(new JdbcPushDownParameter("\"", false, config.getQueryPushDownModule()),
                new BaseJdbcRowExpressionConverter(rowExpressionService),
                new BaseJdbcSqlStatementWriter(new JdbcPushDownParameter("\"", false, config.getQueryPushDownModule())));
    }

    @Override
    protected PlanVisitor<Optional<JdbcQueryGeneratorContext>, Void> getVisitor(TypeManager typeManager)
    {
        return new DataCenterPlanVisitor(typeManager);
    }

    protected class DataCenterPlanVisitor
            extends BaseJdbcPlanVisitor
    {
        public DataCenterPlanVisitor(TypeManager typeManager)
        {
            super(typeManager);
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitPlan(PlanNode node, Void contextIn)
        {
            log.debug(GENERATE_FAILED_LOG, "Don't know how to handle plan node of type " + node);
            return Optional.empty();
        }

        @Override
        public Optional<JdbcQueryGeneratorContext> visitTableScan(TableScanNode node, Void contextIn)
        {
            checkAvailable(node);
            checkArgument(node.getTable().getConnectorHandle() instanceof DataCenterTableHandle,
                    "Expected to find Data Center table handle for the scan node");
            TupleDomain<ColumnHandle> constraint = node.getEnforcedConstraint();
            if (constraint != null && constraint.getDomains().isPresent()) {
                if (!constraint.getDomains().get().isEmpty()) {
                    // Predicate is pushed down
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Cannot push down table scan with predicates pushed down");
                }
            }
            TableHandle tableHandle = node.getTable();
            DataCenterTableHandle dcTableHandle = (DataCenterTableHandle) node.getTable().getConnectorHandle();
            checkArgument(dcTableHandle.getPushDownSql().isEmpty(), "Data center should not have sql before pushdown");
            LinkedHashMap<String, Selection> selections = new LinkedHashMap<>();
            node.getOutputSymbols().forEach(outputColumn -> {
                DataCenterColumnHandle dcColumn = (DataCenterColumnHandle) node.getAssignments().get(outputColumn);
                selections.put(outputColumn.getName(), new Selection(dcColumn.getColumnName(), outputColumn.getName()));
            });
            StringBuilder table = new StringBuilder();
            if (!isNullOrEmpty(dcTableHandle.getCatalogName())) {
                table.append(quote(quote, dcTableHandle.getCatalogName())).append('.');
            }
            if (!isNullOrEmpty(dcTableHandle.getSchemaName())) {
                table.append(quote(quote, dcTableHandle.getSchemaName())).append('.');
            }
            table.append(quote(quote, dcTableHandle.getTableName()));

            JdbcQueryGeneratorContext.Builder contextBuilder = new JdbcQueryGeneratorContext.Builder()
                    .setCatalogName(Optional.of(tableHandle.getCatalogName()))
                    .setTransaction(Optional.of(tableHandle.getTransaction()))
                    .setSchemaTableName(Optional.of(new SchemaTableName(dcTableHandle.getSchemaName(), dcTableHandle.getTableName())))
                    .setSelections(selections)
                    .setFrom(Optional.of(table.toString()));
            // If LIMIT has been push down, add it to context
            if (dcTableHandle.getLimit().isPresent()) {
                contextBuilder.setLimit(dcTableHandle.getLimit());
                contextBuilder.setHasPushDown(true);
            }

            return Optional.of(contextBuilder.build());
        }
    }
}
