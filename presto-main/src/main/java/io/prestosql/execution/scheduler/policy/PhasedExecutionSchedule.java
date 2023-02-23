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
package io.prestosql.execution.scheduler.policy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.StageState;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.StageState.SCHEDULED;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static java.util.function.Function.identity;

@NotThreadSafe
public class PhasedExecutionSchedule
        implements ExecutionSchedule
{
    private final List<Set<SqlStageExecution>> schedulePhases;
    private final Set<SqlStageExecution> activeSources = new HashSet<>();

    public PhasedExecutionSchedule(Collection<SqlStageExecution> stages)
    {
        List<Set<PlanFragmentId>> phases = extractPhases(stages.stream().map(SqlStageExecution::getFragment).collect(toImmutableList()));

        Map<PlanFragmentId, SqlStageExecution> stagesByFragmentId = stages.stream().collect(toImmutableMap(stage -> stage.getFragment().getId(), identity()));

        // create a mutable list of mutable sets of stages, so we can remove completed stages
        schedulePhases = new ArrayList<>();
        for (Set<PlanFragmentId> phase : phases) {
            schedulePhases.add(phase.stream()
                    .map(stagesByFragmentId::get)
                    .collect(Collectors.toCollection(HashSet::new)));
        }
    }

    @Override
    public StagesScheduleResult getStagesToSchedule()
    {
        removeCompletedStages();
        addPhasesIfNecessary();
        if (isFinished()) {
            return new StagesScheduleResult(ImmutableSet.of());
        }
        return new StagesScheduleResult(activeSources);
    }

    private void removeCompletedStages()
    {
        for (Iterator<SqlStageExecution> stageIterator = activeSources.iterator(); stageIterator.hasNext(); ) {
            StageState state = stageIterator.next().getState();
            if (state == SCHEDULED || state == RUNNING || state.isDone()) {
                stageIterator.remove();
            }
        }
    }

    private void addPhasesIfNecessary()
    {
        // we want at least one source distributed phase in the active sources
        if (hasSourceDistributedStage(activeSources)) {
            return;
        }

        while (!schedulePhases.isEmpty()) {
            Set<SqlStageExecution> phase = schedulePhases.remove(0);
            activeSources.addAll(phase);
            if (hasSourceDistributedStage(phase)) {
                return;
            }
        }
    }

    private static boolean hasSourceDistributedStage(Set<SqlStageExecution> phase)
    {
        return phase.stream().anyMatch(stage -> !stage.getFragment().getPartitionedSources().isEmpty());
    }

    @Override
    public boolean isFinished()
    {
        return activeSources.isEmpty() && schedulePhases.isEmpty();
    }

    @VisibleForTesting
    static List<Set<PlanFragmentId>> extractPhases(Collection<PlanFragment> fragments)
    {
        // Build a graph where the plan fragments are vertexes and the edges represent
        // a before -> after relationship.  For example, a join hash build has an edge
        // to the join probe.
        DirectedGraph<PlanFragmentId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        fragments.forEach(fragment -> graph.addVertex(fragment.getId()));

        Visitor visitor = new Visitor(fragments, graph);
        for (PlanFragment fragment : fragments) {
            visitor.processFragment(fragment.getId());
        }

        // Computes all the strongly connected components of the directed graph.
        // These are the "phases" which hold the set of fragments that must be started
        // at the same time to avoid deadlock.
        List<Set<PlanFragmentId>> components = new StrongConnectivityInspector<>(graph).stronglyConnectedSets();

        Map<PlanFragmentId, Set<PlanFragmentId>> componentMembership = new HashMap<>();
        for (Set<PlanFragmentId> component : components) {
            for (PlanFragmentId planFragmentId : component) {
                componentMembership.put(planFragmentId, component);
            }
        }

        // build graph of components (phases)
        DirectedGraph<Set<PlanFragmentId>, DefaultEdge> componentGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        components.forEach(componentGraph::addVertex);
        for (DefaultEdge edge : graph.edgeSet()) {
            PlanFragmentId source = graph.getEdgeSource(edge);
            PlanFragmentId target = graph.getEdgeTarget(edge);

            Set<PlanFragmentId> from = componentMembership.get(source);
            Set<PlanFragmentId> to = componentMembership.get(target);
            if (!from.equals(to)) { // the topological order iterator below doesn't include vertices that have self-edges, so don't add them
                componentGraph.addEdge(from, to);
            }
        }

        return ImmutableList.copyOf(new TopologicalOrderIterator<>(componentGraph));
    }

    private static class Visitor
            extends InternalPlanVisitor<Set<PlanFragmentId>, PlanFragmentId>
    {
        private final Map<PlanFragmentId, PlanFragment> fragments;
        private final DirectedGraph<PlanFragmentId, DefaultEdge> graph;
        private final Map<PlanFragmentId, Set<PlanFragmentId>> fragmentSources = new HashMap<>();

        public Visitor(Collection<PlanFragment> fragments, DirectedGraph<PlanFragmentId, DefaultEdge> graph)
        {
            this.fragments = fragments.stream()
                    .collect(toImmutableMap(PlanFragment::getId, identity()));
            this.graph = graph;
        }

        public Set<PlanFragmentId> processFragment(PlanFragmentId planFragmentId)
        {
            if (fragmentSources.containsKey(planFragmentId)) {
                return fragmentSources.get(planFragmentId);
            }

            Set<PlanFragmentId> fragment = processFragment(fragments.get(planFragmentId));
            fragmentSources.put(planFragmentId, fragment);
            return fragment;
        }

        private Set<PlanFragmentId> processFragment(PlanFragment fragment)
        {
            Set<PlanFragmentId> sources = fragment.getRoot().accept(this, fragment.getId());
            return ImmutableSet.<PlanFragmentId>builder().add(fragment.getId()).addAll(sources).build();
        }

        @Override
        public Set<PlanFragmentId> visitJoin(JoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitJoinOnAggregation(JoinOnAggregationNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitSpatialJoin(SpatialJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitSemiJoin(SemiJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getFilteringSource(), node.getSource(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitIndexJoin(IndexJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getIndexSource(), node.getProbeSource(), currentFragmentId);
        }

        private Set<PlanFragmentId> processJoin(PlanNode build, PlanNode probe, PlanFragmentId currentFragmentId)
        {
            Set<PlanFragmentId> buildSources = build.accept(this, currentFragmentId);
            Set<PlanFragmentId> probeSources = probe.accept(this, currentFragmentId);

            for (PlanFragmentId buildSource : buildSources) {
                for (PlanFragmentId probeSource : probeSources) {
                    graph.addEdge(buildSource, probeSource);
                }
            }

            return ImmutableSet.<PlanFragmentId>builder()
                    .addAll(buildSources)
                    .addAll(probeSources)
                    .build();
        }

        @Override
        public Set<PlanFragmentId> visitRemoteSource(RemoteSourceNode node, PlanFragmentId currentFragmentId)
        {
            ImmutableSet.Builder<PlanFragmentId> sources = ImmutableSet.builder();

            Set<PlanFragmentId> previousFragmentSources = ImmutableSet.of();
            for (PlanFragmentId remoteFragment : node.getSourceFragmentIds()) {
                // this current fragment depends on the remote fragment
                graph.addEdge(currentFragmentId, remoteFragment);

                // get all sources for the remote fragment
                Set<PlanFragmentId> remoteFragmentSources = processFragment(remoteFragment);
                sources.addAll(remoteFragmentSources);

                // For UNION there can be multiple sources.
                // Link the previous source to the current source, so we only
                // schedule one at a time.
                addEdges(previousFragmentSources, remoteFragmentSources);

                previousFragmentSources = remoteFragmentSources;
            }

            return sources.build();
        }

        @Override
        public Set<PlanFragmentId> visitExchange(ExchangeNode node, PlanFragmentId currentFragmentId)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the phased execution scheduler");
            ImmutableSet.Builder<PlanFragmentId> allSources = ImmutableSet.builder();

            // Link the source fragments together, so we only schedule one at a time.
            Set<PlanFragmentId> previousSources = ImmutableSet.of();
            for (PlanNode subPlanNode : node.getSources()) {
                Set<PlanFragmentId> currentSources = subPlanNode.accept(this, currentFragmentId);
                allSources.addAll(currentSources);

                addEdges(previousSources, currentSources);

                previousSources = currentSources;
            }

            return allSources.build();
        }

        @Override
        public Set<PlanFragmentId> visitUnion(UnionNode node, PlanFragmentId currentFragmentId)
        {
            ImmutableSet.Builder<PlanFragmentId> allSources = ImmutableSet.builder();

            // Link the source fragments together, so we only schedule one at a time.
            Set<PlanFragmentId> previousSources = ImmutableSet.of();
            for (PlanNode subPlanNode : node.getSources()) {
                Set<PlanFragmentId> currentSources = subPlanNode.accept(this, currentFragmentId);
                allSources.addAll(currentSources);

                addEdges(previousSources, currentSources);

                previousSources = currentSources;
            }

            return allSources.build();
        }

        @Override
        public Set<PlanFragmentId> visitPlan(PlanNode node, PlanFragmentId currentFragmentId)
        {
            List<PlanNode> sources = node.getSources();
            if (sources.isEmpty()) {
                return ImmutableSet.of(currentFragmentId);
            }
            if (sources.size() == 1) {
                return sources.get(0).accept(this, currentFragmentId);
            }
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private void addEdges(Set<PlanFragmentId> sourceFragments, Set<PlanFragmentId> targetFragments)
        {
            for (PlanFragmentId targetFragment : targetFragments) {
                for (PlanFragmentId sourceFragment : sourceFragments) {
                    graph.addEdge(sourceFragment, targetFragment);
                }
            }
        }
    }
}
