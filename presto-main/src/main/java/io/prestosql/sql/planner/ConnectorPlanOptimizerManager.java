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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.transformValues;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class ConnectorPlanOptimizerManager
{
    private final Map<CatalogName, ConnectorPlanOptimizerProvider> planOptimizerProviders = new ConcurrentHashMap<>();

    @Inject
    public ConnectorPlanOptimizerManager() {}

    public void addPlanOptimizerProvider(CatalogName catalogName, ConnectorPlanOptimizerProvider planOptimizerProvider)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(planOptimizerProvider, "planOptimizerProvider is null");
        checkArgument(planOptimizerProviders.putIfAbsent(catalogName, planOptimizerProvider) == null,
                "ConnectorPlanOptimizerProvider for catalog '%s' is already registered", catalogName);
    }

    public void removePlanOptimizerProvider(CatalogName catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        planOptimizerProviders.remove(catalogName);
    }

    public Map<CatalogName, Set<ConnectorPlanOptimizer>> getOptimizers(PlanPhase phase)
    {
        switch (phase) {
            case LOGICAL:
                return ImmutableMap.copyOf(transformValues(planOptimizerProviders, ConnectorPlanOptimizerProvider::getLogicalPlanOptimizers));
            case PHYSICAL:
                return ImmutableMap.copyOf(transformValues(planOptimizerProviders, ConnectorPlanOptimizerProvider::getPhysicalPlanOptimizers));
            default:
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unknown plan phase " + phase);
        }
    }

    public enum PlanPhase
    {
        LOGICAL, PHYSICAL
    }
}
