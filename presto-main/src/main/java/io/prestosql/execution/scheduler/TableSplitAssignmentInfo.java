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

/**
 * Singleton Class for storing Split Assignment Information used for Reuse-Exchange optimizer
 */
public class TableSplitAssignmentInfo
{
    private static TableSplitAssignmentInfo tableSplitAssignmentInfo;
    private HashMap<Integer, Multimap<InternalNode, Split>> tableSlotSplitAssignment;
    private HashMap<Integer, HashMap<SplitKey, InternalNode>> perTableSlotSplitKeyNodeAssignment;
    private static final Logger log = Logger.get(TableSplitAssignmentInfo.class);

    /**
     * Private constructor. Initialized exactly once
     */
    private TableSplitAssignmentInfo()
    {
        tableSlotSplitAssignment = new HashMap<>();
        perTableSlotSplitKeyNodeAssignment = new HashMap<>();
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
     * Slot number uniquely identifies a producer-consumer pair for a reused table
     * @return HashMap containing slot number as key and node-split multimap as value for all tables in this query
     */
    public HashMap<Integer, Multimap<InternalNode, Split>> getTableSlotSplitAssignment()
    {
        return tableSlotSplitAssignment;
    }

    /**
     * Store the assignment multimap with the corresponding slot number
     * @param qualifiedTableName name of the table which is as a producer(reads data from disk for the first time)
     * @param slot unique identifier for producer-consumer pair for a reused table
     * @param assignmentInformation node-split assignment multimap created as part of the stage that processes this table
     */
    public void setTableSplitAssignment(QualifiedObjectName qualifiedTableName, int slot, Multimap<InternalNode, Split> assignmentInformation)
    {
        Multimap<InternalNode, Split> assignmentMap = HashMultimap.create();
        assignmentMap.putAll(assignmentInformation);
        this.tableSlotSplitAssignment.put(slot, assignmentMap);
        setPerTablesplitKeyNodeAssignment(qualifiedTableName, slot, assignmentMap);
    }

    /**
     * @param slot unique identifier for producer-consumer pair for a reused table
     * @return map of splitkey-node mapping for corresponding slot number
     * NOTE: Works only with Hive data as other connectors don't support SplitKey currently
     */
    public HashMap<SplitKey, InternalNode> getSplitKeyNodeAssignment(int slot)
    {
        return perTableSlotSplitKeyNodeAssignment.get(slot);
    }

    /**
     * Store the inverted assignment information [Split-Node mapping] for a given slot number
     * @param qualifiedTableName name of the table which is as a producer(reads data from disk for the first time)
     * @param slot unique identifier for producer-consumer pair for a reused table
     * @param assignmentInformation node-split assignment multimap created as part of the stage that processes this table
     * NOTE: Works only with Hive data as other connectors don't support SplitKey currently
     */
    private void setPerTablesplitKeyNodeAssignment(QualifiedObjectName qualifiedTableName, int slot, Multimap<InternalNode, Split> assignmentInformation)
    {
        String catalog = qualifiedTableName.getCatalogName();
        String schema = qualifiedTableName.getSchemaName();
        String table = qualifiedTableName.getObjectName();
        HashMap<SplitKey, InternalNode> splitKeyNodeAssignment;
        try {
            splitKeyNodeAssignment = perTableSlotSplitKeyNodeAssignment.get(slot);
            if (splitKeyNodeAssignment == null) {
                splitKeyNodeAssignment = new HashMap<>();
            }

            for (InternalNode node : assignmentInformation.keySet()) {
                Collection<Split> assigmentSplits = assignmentInformation.get(node);
                for (Split assigmentSplit : assigmentSplits) {
                    if (assigmentSplit.getConnectorSplit().getSplitNum() > 1) {
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

            perTableSlotSplitKeyNodeAssignment.put(slot, splitKeyNodeAssignment);
        }
        catch (NotImplementedException e) {
            log.error("Not a Hive Split! Other Connector Splits not supported currently. Error: " + e);
            throw new UnsupportedOperationException("Not a Hive Split! Other Connector Splits not supported currently. Error: " + e);
        }
    }
}
