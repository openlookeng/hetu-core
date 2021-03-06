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
package io.prestosql.execution.scheduler;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.prestosql.execution.SplitKey;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.Split;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Singleton Class for storing Split Assignment Information used for Reuse-Exchange optimizer
 */
public class TableSplitAssignmentInfo
{
    private static TableSplitAssignmentInfo tableSplitAssignmentInfo;
    private HashMap<UUID, Multimap<InternalNode, Split>> reuseTableScanMappingIdSplitAssignmentMap;
    private HashMap<UUID, HashMap<SplitKey, InternalNode>> perTableReuseTableScanMappingIdSplitKeyNodeAssignment;
    private static final Logger log = Logger.get(TableSplitAssignmentInfo.class);

    /**
     * Private constructor. Initialized exactly once
     */
    private TableSplitAssignmentInfo()
    {
        reuseTableScanMappingIdSplitAssignmentMap = new HashMap<>();
        perTableReuseTableScanMappingIdSplitKeyNodeAssignment = new HashMap<>();
    }

    /**
     * @return object of this class
     */
    public static TableSplitAssignmentInfo getInstance()
    {
        if (tableSplitAssignmentInfo == null) {
            tableSplitAssignmentInfo = new TableSplitAssignmentInfo();
        }
        return tableSplitAssignmentInfo;
    }

    /**
     * reuseTableScanMappingId number uniquely identifies a producer-consumer pair for a reused table
     * @return HashMap containing reuseTableScanMappingId number as key and node-split multimap as value for all tables in this query
     */
    public HashMap<UUID, Multimap<InternalNode, Split>> getReuseTableScanMappingIdSplitAssignmentMap()
    {
        return reuseTableScanMappingIdSplitAssignmentMap;
    }

    /**
     * Store the assignment multimap with the corresponding reuseTableScanMappingId number
     * @param qualifiedTableName name of the table which is as a producer(reads data from disk for the first time)
     * @param reuseTableScanMappingId unique identifier for producer-consumer pair for a reused table
     * @param assignmentInformation node-split assignment multimap created as part of the stage that processes this table
     */
    public void setTableSplitAssignment(QualifiedObjectName qualifiedTableName, UUID reuseTableScanMappingId, Multimap<InternalNode, Split> assignmentInformation)
    {
        Multimap<InternalNode, Split> assignmentMap = HashMultimap.create();
        assignmentMap.putAll(assignmentInformation);
        this.reuseTableScanMappingIdSplitAssignmentMap.put(reuseTableScanMappingId, assignmentMap);
        setPerTablesplitKeyNodeAssignment(qualifiedTableName, reuseTableScanMappingId, assignmentMap);
    }

    /**
     * @param reuseTableScanMappingId unique identifier for producer-consumer pair for a reused table
     * @return map of splitkey-node mapping for corresponding reuseTableScanMappingId number
     * NOTE: Works only with Hive data as other connectors don't support SplitKey currently
     */
    public HashMap<SplitKey, InternalNode> getSplitKeyNodeAssignment(UUID reuseTableScanMappingId)
    {
        return perTableReuseTableScanMappingIdSplitKeyNodeAssignment.get(reuseTableScanMappingId);
    }

    /**
     * Store the inverted assignment information [Split-Node mapping] for a given reuseTableScanMappingId number
     * @param qualifiedTableName name of the table which is as a producer(reads data from disk for the first time)
     * @param reuseTableScanMappingId unique identifier for producer-consumer pair for a reused table
     * @param assignmentInformation node-split assignment multimap created as part of the stage that processes this table
     * NOTE: Works only with Hive data as other connectors don't support SplitKey currently
     */
    private void setPerTablesplitKeyNodeAssignment(QualifiedObjectName qualifiedTableName, UUID reuseTableScanMappingId, Multimap<InternalNode, Split> assignmentInformation)
    {
        String catalog = qualifiedTableName.getCatalogName();
        String schema = qualifiedTableName.getSchemaName();
        String table = qualifiedTableName.getObjectName();
        HashMap<SplitKey, InternalNode> splitKeyNodeAssignment;
        try {
            splitKeyNodeAssignment = perTableReuseTableScanMappingIdSplitKeyNodeAssignment.get(reuseTableScanMappingId);
            if (splitKeyNodeAssignment == null) {
                splitKeyNodeAssignment = new HashMap<>();
            }

            for (InternalNode node : assignmentInformation.keySet()) {
                Collection<Split> assigmentSplits = assignmentInformation.get(node);
                for (Split assigmentSplit : assigmentSplits) {
                    if (assigmentSplit.getConnectorSplit().getSplitCount() > 1) {
                        for (Split unwrappedSplit : assigmentSplit.getSplits()) {
                            SplitKey splitKey = new SplitKey(unwrappedSplit, catalog, schema, table);
                            splitKeyNodeAssignment.put(splitKey, node);
                        }
                    }
                    else {
                        SplitKey splitKey = new SplitKey(assigmentSplit, catalog, schema, table);
                        splitKeyNodeAssignment.put(splitKey, node);
                    }
                }
            }

            perTableReuseTableScanMappingIdSplitKeyNodeAssignment.put(reuseTableScanMappingId, splitKeyNodeAssignment);
        }
        catch (NotImplementedException e) {
            log.error("Unsupported split type: " + e);
            throw new UnsupportedOperationException("Unsupported split type: " + e);
        }
    }

    public void removeFromTableSplitAssignmentInfo(List<UUID> uuidList)
    {
        for (UUID uuid : uuidList) {
            if (reuseTableScanMappingIdSplitAssignmentMap.containsKey(uuid)) {
                reuseTableScanMappingIdSplitAssignmentMap.remove(uuid);
            }
            if (perTableReuseTableScanMappingIdSplitKeyNodeAssignment.containsKey(uuid)) {
                perTableReuseTableScanMappingIdSplitKeyNodeAssignment.remove(uuid);
            }
        }
    }
}
