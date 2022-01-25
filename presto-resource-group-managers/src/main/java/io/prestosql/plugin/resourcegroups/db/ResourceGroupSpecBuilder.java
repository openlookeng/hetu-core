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
package io.prestosql.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.prestosql.plugin.resourcegroups.ResourceGroupNameTemplate;
import io.prestosql.plugin.resourcegroups.ResourceGroupSpec;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResourceGroupSpecBuilder
{
    private final long id;
    private final ResourceGroupNameTemplate nameTemplate;
    private final String softMemoryLimit;
    private final String softReservedMemory; // Hetu: add parameter softReservedMemory
    private final int maxQueued;
    private final Optional<Integer> softConcurrencyLimit;
    private final int hardConcurrencyLimit;
    private final Optional<Integer> hardReservedConcurrency; // Hetu: add parameter hardReservedConcurrency
    private final Optional<String> schedulingPolicy;
    private final Optional<Integer> schedulingWeight;
    private final Optional<Boolean> jmxExport;
    private final Optional<Duration> softCpuLimit;
    private final Optional<Duration> hardCpuLimit;
    private final Optional<String> killPolicy;
    private final Optional<Long> parentId;
    private final ImmutableList.Builder<ResourceGroupSpec> subGroups = ImmutableList.builder();

    ResourceGroupSpecBuilder(
            long id,
            ResourceGroupNameTemplate nameTemplate,
            String softMemoryLimit,
            String softReservedMemory,
            int maxQueued,
            Optional<Integer> softConcurrencyLimit,
            int hardConcurrencyLimit,
            Optional<Integer> hardReservedConcurrency,
            Optional<String> schedulingPolicy,
            Optional<Integer> schedulingWeight,
            Optional<Boolean> jmxExport,
            Optional<String> softCpuLimit,
            Optional<String> hardCpuLimit,
            Optional<String> killPolicy,
            Optional<Long> parentId)
    {
        this.id = id;
        this.nameTemplate = nameTemplate;
        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        // Hetu: initialize softReservedMemory
        this.softReservedMemory = requireNonNull(softReservedMemory, "softReservedMemory is null");
        this.maxQueued = maxQueued;
        this.softConcurrencyLimit = requireNonNull(softConcurrencyLimit, "softConcurrencyLimit is null");
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.hardReservedConcurrency = hardReservedConcurrency; // Hetu: initialize hardReservedConcurrency
        this.schedulingPolicy = requireNonNull(schedulingPolicy, "schedulingPolicy is null");
        this.schedulingWeight = schedulingWeight;
        this.jmxExport = requireNonNull(jmxExport, "jmxExport is null");
        this.softCpuLimit = requireNonNull(softCpuLimit, "softCpuLimit is null").map(Duration::valueOf);
        this.hardCpuLimit = requireNonNull(hardCpuLimit, "hardCpuLimit is null").map(Duration::valueOf);
        this.killPolicy = killPolicy;
        this.parentId = parentId;
    }

    public long getId()
    {
        return id;
    }

    public ResourceGroupNameTemplate getNameTemplate()
    {
        return nameTemplate;
    }

    public Optional<Duration> getSoftCpuLimit()
    {
        return softCpuLimit;
    }

    public Optional<Duration> getHardCpuLimit()
    {
        return hardCpuLimit;
    }

    public Optional<String> getKillPolicy()
    {
        return killPolicy;
    }

    public Optional<Long> getParentId()
    {
        return parentId;
    }

    public void addSubGroup(ResourceGroupSpec subGroup)
    {
        subGroups.add(subGroup);
    }

    public ResourceGroupSpec build()
    {
        return new ResourceGroupSpec(
                nameTemplate,
                softMemoryLimit,
                Optional.ofNullable(softReservedMemory), //Hetu: add parameter softReservedMemory
                maxQueued,
                softConcurrencyLimit,
                Optional.of(hardConcurrencyLimit),
                hardReservedConcurrency, //Hetu: add parameter hardReservedConcurrency
                Optional.empty(),
                schedulingPolicy,
                schedulingWeight,
                Optional.of(subGroups.build()),
                jmxExport,
                softCpuLimit,
                hardCpuLimit,
                killPolicy);
    }

    public static class Mapper
            implements RowMapper<ResourceGroupSpecBuilder>
    {
        @Override
        public ResourceGroupSpecBuilder map(ResultSet resultSet, StatementContext context)
                throws SQLException
        {
            long resourceGroupId = resultSet.getLong("resource_group_id");
            ResourceGroupNameTemplate resourceGroupNameTemplate = new ResourceGroupNameTemplate(resultSet.getString("name"));
            String memoryLimitSoft = resultSet.getString("soft_memory_limit");
            String reservedMemorySoft = resultSet.getString("soft_reserved_memory");
            int queuedMax = resultSet.getInt("max_queued");
            Optional<Integer> concurrencyLimitSoft = Optional.of(resultSet.getInt("soft_concurrency_limit"));
            if (resultSet.wasNull()) {
                concurrencyLimitSoft = Optional.empty();
            }
            int hConcurrencyLimit = resultSet.getInt("hard_concurrency_limit");
            Optional<String> policy = Optional.ofNullable(resultSet.getString("scheduling_policy"));
            Optional<Integer> weight = Optional.of(resultSet.getInt("scheduling_weight"));
            if (resultSet.wasNull()) {
                weight = Optional.empty();
            }
            Optional<Boolean> export = Optional.of(resultSet.getBoolean("jmx_export"));
            if (resultSet.wasNull()) {
                export = Optional.empty();
            }
            Optional<String> softLimit = Optional.ofNullable(resultSet.getString("soft_cpu_limit"));
            Optional<String> hardLimit = Optional.ofNullable(resultSet.getString("hard_cpu_limit"));
            Optional<Long> parent = Optional.of(resultSet.getLong("parent"));
            if (resultSet.wasNull()) {
                parent = Optional.empty();
            }
            Optional<Integer> reservedConcurrency = Optional.of(resultSet.getInt("hard_reserved_concurrency"));
            if (resultSet.wasNull()) {
                reservedConcurrency = Optional.empty();
            }

            Optional<String> kPolicy = Optional.ofNullable(resultSet.getString("kill_policy"));
            if (resultSet.wasNull()) {
                kPolicy = Optional.empty();
            }

            return new ResourceGroupSpecBuilder(
                    resourceGroupId,
                    resourceGroupNameTemplate,
                    memoryLimitSoft,
                    reservedMemorySoft,
                    queuedMax,
                    concurrencyLimitSoft,
                    hConcurrencyLimit,
                    reservedConcurrency,
                    policy,
                    weight,
                    export,
                    softLimit,
                    hardLimit,
                    kPolicy,
                    parent);
        }
    }
}
