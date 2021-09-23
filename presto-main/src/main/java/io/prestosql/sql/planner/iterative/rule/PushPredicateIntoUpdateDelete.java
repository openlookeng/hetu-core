/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.tree.Expression;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.prestosql.sql.planner.plan.Patterns.tableWriterNode;

public class PushPredicateIntoUpdateDelete
        implements Rule<TableWriterNode>
{
    private static final Pattern<TableWriterNode> PATTERN = tableWriterNode()
            .matching(tableWriter ->
                    tableWriter.getTarget() instanceof TableWriterNode.UpdateDeleteReference);

    private final Metadata metadata;

    public PushPredicateIntoUpdateDelete(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<TableWriterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return true;
    }

    @Override
    public Result apply(TableWriterNode writerNode, Captures captures, Context context)
    {
        TableWriterNode.WriterTarget target = writerNode.getTarget();
        if (target instanceof TableWriterNode.UpdateDeleteReference) {
            TableWriterNode.UpdateDeleteReference updateReference = (TableWriterNode.UpdateDeleteReference) target;
            if (!updateReference.getConstraint().isPresent()) {
                return Result.empty();
            }
            TableHandle tableHandle = pushPredicateToUpdateDelete(updateReference.getHandle(), updateReference.getColumnAssignments(),
                    updateReference.getConstraint().get(), context.getSession(), context.getSymbolAllocator().getTypes(), metadata);
            if (tableHandle != null) {
                updateReference.setHandle(tableHandle);
            }
        }
        return Result.empty(); //directly modified the target
    }

    private static TableHandle pushPredicateToUpdateDelete(
            TableHandle handle,
            Map<Symbol, ColumnHandle> columns,
            Expression predicate,
            Session session,
            TypeProvider types,
            Metadata metadata)
    {
        Expression deterministicPredicate = filterDeterministicConjuncts(predicate);

        ExpressionDomainTranslator.ExtractionResult decomposedPredicate = ExpressionDomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                types);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(columns::get);

        Constraint constraint = new Constraint(newDomain);
        Set<ColumnHandle> allColumnHandles = new HashSet<>();
        columns.values().stream().forEach(allColumnHandles::add);
        Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(
                session, handle, constraint, ImmutableList.of(), allColumnHandles, true);
        if (result.isPresent()) {
            return result.get().getHandle();
        }
        return null;
    }
}
