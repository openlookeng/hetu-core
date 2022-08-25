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
package io.prestosql.execution.resourcegroups;

import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Classes implementing this interface must be thread safe. That is, all the methods listed below
 * may be called concurrently from any thread.
 */
@ThreadSafe
public interface ResourceGroupManager<C>
{
    void submit(ManagedQueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor);

    SelectionContext<C> selectGroup(SelectionCriteria criteria);

    ResourceGroupInfo getResourceGroupInfo(ResourceGroupId id);

    List<ResourceGroupInfo> getPathToRoot(ResourceGroupId id);

    void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory);

    void loadConfigurationManager()
            throws Exception;

    default long getCachedMemoryUsage(ResourceGroupId resourceGroupId)
    {
        return 0;
    }

    default long getSoftReservedMemory(ResourceGroupId resourceGroupId)
    {
        return Long.MAX_VALUE;
    }

    default boolean isGroupRegistered(ResourceGroupId resourceGroupId)
    {
        return false;
    }

    default long getSoftCpuLimit(ResourceGroupId resourceGroupId)
    {
        return -1;
    }

    default long getHardCpuLimit(ResourceGroupId resourceGroupId)
    {
        return -1;
    }
}
