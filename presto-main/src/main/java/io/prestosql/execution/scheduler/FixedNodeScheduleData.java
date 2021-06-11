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

package io.prestosql.execution.scheduler;

import io.prestosql.metadata.InternalNode;

import java.util.Set;

public class FixedNodeScheduleData
{
    private int splitCount;
    private Set<InternalNode> assignedNodes;

    public FixedNodeScheduleData(int splitCount, Set<InternalNode> assignedNodes)
    {
        this.splitCount = splitCount;
        this.assignedNodes = assignedNodes;
    }

    public int getSplitCount()
    {
        return splitCount;
    }

    public void setSplitCount(int splitCount)
    {
        this.splitCount = splitCount;
    }

    public void updateSplitCount(int splitCount)
    {
        this.splitCount += splitCount;
    }

    public Set<InternalNode> getAssignedNodes()
    {
        return assignedNodes;
    }

    public void setAssignedNodes(Set<InternalNode> assignedNodes)
    {
        this.assignedNodes = assignedNodes;
    }

    public void updateAssignedNodes(Set<InternalNode> nodes)
    {
        assignedNodes.addAll(nodes);
    }
}
