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
package io.prestosql.utils;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.analyzer.FeaturesConfig.RedistributeWritesType;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.plan.TableWriterNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.sql.analyzer.FeaturesConfig.RedistributeWritesType.PARTITIONED;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;

public class WriteExchangePartitioner
{
    private static final String PARTITIONED_BY = "partitioned_by";

    private WriteExchangePartitioner()
    {
    }

    /**
     * Create partition scheme to redistribute writes based on the partition key.
     *
     * @param metadata the metadata
     * @param session the session
     * @param node the TableWriterNode node
     * @param source the source node
     * @param redistributeWritesType the redistribute writes type
     * @return optional PartitioningScheme
     */
    public static Optional<PartitioningScheme> createPartitioningScheme(Metadata metadata, Session session, TableWriterNode node, PlanNode source, RedistributeWritesType redistributeWritesType)
    {
        Map<String, Object> properties = null;
        if (node.getTarget() instanceof TableWriterNode.CreateReference) {
            // create table with partition_by
            properties = ((TableWriterNode.CreateReference) node.getTarget()).getTableMetadata().getProperties();
        }
        else if (node.getTarget() instanceof TableWriterNode.InsertReference) {
            // insert into a partitioned table
            properties = metadata.getTableMetadata(session, ((TableWriterNode.InsertReference) node.getTarget()).getHandle()).getMetadata().getProperties();
        }
        if (properties != null && !node.getColumns().isEmpty()) {
            // Check if the property has partitioned_by column information
            Object partitionKeys = properties.get(PARTITIONED_BY);
            if (partitionKeys instanceof List && !((List) partitionKeys).isEmpty() && redistributeWritesType == PARTITIONED) {
                // Create fixed hash distribution partition scheme.
                // In Presto, partition columns comes last. If there are more than one columns,
                // only the last column is used to create partition key
                Symbol partitionColumn = node.getColumns().get(node.getColumns().size() - 1);
                return Optional.of(new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(partitionColumn)),
                        source.getOutputSymbols(),
                        Optional.of(partitionColumn),
                        false,
                        Optional.empty()));
            }
        }
        // The default partition scheme
        return Optional.of(new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), source.getOutputSymbols()));
    }
}
