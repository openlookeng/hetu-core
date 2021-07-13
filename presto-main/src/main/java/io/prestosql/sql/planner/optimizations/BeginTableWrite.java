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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TableWriterNode.CreateReference;
import io.prestosql.sql.planner.plan.TableWriterNode.CreateTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteAsInsertReference;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteAsInsertTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.InsertReference;
import io.prestosql.sql.planner.plan.TableWriterNode.InsertTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.UpdateReference;
import io.prestosql.sql.planner.plan.TableWriterNode.UpdateTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.VacuumTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.VacuumTargetReference;
import io.prestosql.sql.planner.plan.TableWriterNode.WriterTarget;
import io.prestosql.sql.planner.plan.UpdateNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.stream.Collectors.toSet;

/*
 * Major HACK alert!!!
 *
 * This logic should be invoked on query start, not during planning. At that point, the token
 * returned by beginCreate/beginInsert should be handed down to tasks in a mapping separate
 * from the plan that links plan nodes to the corresponding token.
 */
public class BeginTableWrite
        implements PlanOptimizer
{
    private final Metadata metadata;

    public BeginTableWrite(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(session), plan, new Context());
    }

    private class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;

        public Rewriter(Session session)
        {
            this.session = session;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Context> context)
        {
            // Part of the plan should be an Optional<StateChangeListener<QueryState>> and this
            // callback can create the table and abort the table creation if the query fails.

            WriterTarget writerTarget = context.get().getMaterializedHandle(node.getTarget()).get();
            return new TableWriterNode(
                    node.getId(),
                    node.getSource().accept(this, context),
                    writerTarget,
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getPartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitVacuumTable(VacuumTableNode node, RewriteContext<Context> context)
        {
            VacuumTarget vacuumTarget = (VacuumTarget) context.get().getMaterializedHandle(node.getTarget()).get();
            return new VacuumTableNode(node.getId(),
                    node.getTable(),
                    vacuumTarget,
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getPartition(),
                    node.isFull(),
                    node.getInputSymbols(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Context> context)
        {
            DeleteTarget deleteTarget = (DeleteTarget) context.get().handle.get();
            return new DeleteNode(
                    node.getId(),
                    rewriteModifyTableScan(node.getSource(), deleteTarget.getHandle()),
                    deleteTarget,
                    node.getRowId(),
                    node.getOutputSymbols());
        }

        @Override
        public PlanNode visitUpdate(UpdateNode node, RewriteContext<Context> context)
        {
            UpdateTarget updateTarget = (UpdateTarget) context.get().handle.get();
            return new UpdateNode(
                    node.getId(),
                    rewriteModifyTableScan(node.getSource(), updateTarget.getHandle()),
                    updateTarget,
                    node.getRowId(),
                    node.getColumnValueAndRowIdSymbols(),
                    node.getOutputSymbols(),
                    node.getUpdateColumnExpression());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Context> context)
        {
            PlanNode child = node.getSource();
            child = child.accept(this, context);

            StatisticsWriterNode.WriteStatisticsHandle analyzeHandle =
                    new StatisticsWriterNode.WriteStatisticsHandle(metadata.beginStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsReference) node.getTarget()).getHandle()));

            return new StatisticsWriterNode(
                    node.getId(),
                    child,
                    analyzeHandle,
                    node.getRowCountSymbol(),
                    node.isRowCountEnabled(),
                    node.getDescriptor());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Context> context)
        {
            PlanNode child = node.getSource();

            WriterTarget originalTarget = getTarget(child);
            WriterTarget newTarget = createWriterTarget(originalTarget);

            context.get().addMaterializedHandle(originalTarget, newTarget);
            child = child.accept(this, context);

            return new TableFinishNode(
                    node.getId(),
                    child,
                    newTarget,
                    node.getRowCountSymbol(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        public WriterTarget getTarget(PlanNode node)
        {
            if (node instanceof TableWriterNode) {
                return ((TableWriterNode) node).getTarget();
            }
            if (node instanceof VacuumTableNode) {
                return ((VacuumTableNode) node).getTarget();
            }
            if (node instanceof DeleteNode) {
                DeleteNode deleteNode = (DeleteNode) node;
                DeleteTarget delete = deleteNode.getTarget();
                return new DeleteTarget(
                        locateTableScanHandle(deleteNode.getSource()),
                        delete.getSchemaTableName());
            }
            if (node instanceof UpdateNode) {
                UpdateNode updateNode = (UpdateNode) node;
                UpdateTarget update = updateNode.getTarget();
                return new UpdateTarget(
                        locateTableScanHandle(updateNode.getSource()),
                        update.getSchemaTableName(),
                        update.getUpdatedColumns(),
                        update.getUpdatedColumnTypes());
            }
            if (node instanceof ExchangeNode || node instanceof UnionNode) {
                Set<WriterTarget> writerTargets = node.getSources().stream()
                        .map(this::getTarget)
                        .collect(toSet());
                return getOnlyElement(writerTargets);
            }
            throw new IllegalArgumentException("Invalid child for TableCommitNode: " + node.getClass().getSimpleName());
        }

        private WriterTarget createWriterTarget(WriterTarget target)
        {
            // TODO: begin these operations in pre-execution step, not here
            // TODO: we shouldn't need to store the schemaTableName in the handles, but there isn't a good way to pass this around with the current architecture
            if (target instanceof CreateReference) {
                CreateReference create = (CreateReference) target;
                return new CreateTarget(metadata.beginCreateTable(session, create.getCatalog(), create.getTableMetadata(), create.getLayout()), create.getTableMetadata().getTable());
            }
            if (target instanceof InsertReference) {
                InsertReference insert = (InsertReference) target;
                return new InsertTarget(metadata.beginInsert(session, insert.getHandle(), insert.isOverwrite()), metadata.getTableMetadata(session, insert.getHandle()).getTable(), insert.isOverwrite());
            }
            if (target instanceof DeleteTarget) {
                DeleteTarget delete = (DeleteTarget) target;
                return new DeleteTarget(metadata.beginDelete(session, delete.getHandle()), delete.getSchemaTableName());
            }
            if (target instanceof UpdateTarget) {
                UpdateTarget update = (UpdateTarget) target;
                return new UpdateTarget(
                        metadata.beginUpdate(session, update.getHandle(), ImmutableList.copyOf(update.getUpdatedColumnTypes())),
                        update.getSchemaTableName(),
                        update.getUpdatedColumns(),
                        update.getUpdatedColumnTypes());
            }
            if (target instanceof DeleteAsInsertReference) {
                DeleteAsInsertReference delete = (DeleteAsInsertReference) target;
                return new DeleteAsInsertTarget(metadata.beginDeletAsInsert(session, delete.getHandle()), metadata.getTableMetadata(session, delete.getHandle()).getTable());
            }
            if (target instanceof UpdateReference) {
                UpdateReference update = (UpdateReference) target;
                return new TableWriterNode.UpdateAsInsertTarget(metadata.beginUpdateAsInsert(session, update.getHandle()), metadata.getTableMetadata(session, update.getHandle()).getTable());
            }
            if (target instanceof VacuumTargetReference) {
                VacuumTargetReference vacuum = (VacuumTargetReference) target;
                return new VacuumTarget(metadata.beginVacuum(session, vacuum.getHandle(), vacuum.isFull(), vacuum.isUnify(), vacuum.getPartition()),
                        metadata.getTableMetadata(session, vacuum.getHandle()).getTable());
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        private TableHandle locateTableScanHandle(PlanNode node)
        {
            if (node instanceof TableScanNode) {
                return ((TableScanNode) node).getTable();
            }
            if (node instanceof FilterNode) {
                return locateTableScanHandle(((FilterNode) node).getSource());
            }
            if (node instanceof ProjectNode) {
                return locateTableScanHandle(((ProjectNode) node).getSource());
            }
            if (node instanceof SemiJoinNode) {
                return locateTableScanHandle(((SemiJoinNode) node).getSource());
            }
            if (node instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node;
                if (joinNode.getType() == JoinNode.Type.INNER) {
                    return locateTableScanHandle(joinNode.getLeft());
                }
            }
            throw new IllegalArgumentException("Invalid descendant for DeleteNode or UpdateNode" + node.getClass().getName());
        }

        private PlanNode rewriteModifyTableScan(PlanNode node, TableHandle handle)
        {
            if (node instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) node;
                return new TableScanNode(
                        scan.getId(),
                        handle,
                        scan.getOutputSymbols(),
                        scan.getAssignments(),
                        scan.getEnforcedConstraint(),
                        scan.getPredicate(), scan.getStrategy(),
                        scan.getReuseTableScanMappingId(), 0, scan.isForDelete());
            }

            if (node instanceof FilterNode) {
                PlanNode source = rewriteModifyTableScan(((FilterNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof ProjectNode) {
                PlanNode source = rewriteModifyTableScan(((ProjectNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof SemiJoinNode) {
                PlanNode source = rewriteModifyTableScan(((SemiJoinNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source, ((SemiJoinNode) node).getFilteringSource()));
            }
            if (node instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node;
                if (joinNode.getType() == JoinNode.Type.INNER) {
                    PlanNode source = rewriteModifyTableScan(joinNode.getLeft(), handle);
                    return replaceChildren(node, ImmutableList.of(source, joinNode.getRight()));
                }
            }
            throw new IllegalArgumentException("Invalid descendant for DeleteNode or UpdateNode: " + node.getClass().getName());
        }
    }

    public static class Context
    {
        private Optional<WriterTarget> handle = Optional.empty();
        private Optional<WriterTarget> materializedHandle = Optional.empty();

        public void addMaterializedHandle(WriterTarget handle, WriterTarget materializedHandle)
        {
            checkState(!this.handle.isPresent(), "can only have one WriterTarget in a subtree");
            this.handle = Optional.of(handle);
            this.materializedHandle = Optional.of(materializedHandle);
        }

        public Optional<WriterTarget> getMaterializedHandle(WriterTarget handle)
        {
            checkState(this.handle.get().equals(handle), "can't find materialized handle for WriterTarget");
            return materializedHandle;
        }
    }
}
