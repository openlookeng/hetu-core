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
package io.prestosql.snapshot;

import io.airlift.log.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A task uses this to keep track of capture/restore status of its components (e.g. operators) for a given snapshot id.
 * A query uses this to keep track of capture/restore status of its tasks for a given snapshot id.
 */
public class SnapshotComponentCounter<T>
{
    private static final Logger log = Logger.get(SnapshotComponentCounter.class);

    // Function to verify if snapshot is complete for query components
    private final Function<Set<T>, Boolean> checkComplete;
    // Total number of components needs to be captured/restored, used when the above function is not present,
    // to verify if snapshot is complete for task components.
    private final int totalComponentCount;
    // State for each component being tracked
    private final Map<T, ComponentState> componentMap = new LinkedHashMap<>();
    // Overall result
    private SnapshotResult snapshotResult = SnapshotResult.IN_PROGRESS;

    public SnapshotComponentCounter(int totalComponentCount)
    {
        this.totalComponentCount = totalComponentCount;
        this.checkComplete = null;
    }

    public SnapshotComponentCounter(Function<Set<T>, Boolean> checkComplete)
    {
        this.totalComponentCount = 0;
        this.checkComplete = checkComplete;
    }

    // Returns whether a change was made
    public synchronized boolean updateComponent(T stateId, ComponentState state)
    {
        ComponentState oldState = componentMap.get(stateId);
        if (oldState != null && oldState.ordinal() >= state.ordinal()) {
            // Already have a result for this component, then only allow changes to a more "severe" state
            return false;
        }
        componentMap.put(stateId, state);

        // update snapshotRestoreResult
        if (state == ComponentState.FAILED_FATAL) {
            snapshotResult = SnapshotResult.IN_PROGRESS_FAILED_FATAL;
        }
        else if (state == ComponentState.FAILED && snapshotResult != SnapshotResult.FAILED_FATAL && snapshotResult != SnapshotResult.IN_PROGRESS_FAILED_FATAL) {
            snapshotResult = SnapshotResult.IN_PROGRESS_FAILED;
        }

        if (checkComplete != null) {
            if (checkComplete.apply(componentMap.keySet())) {
                doneResult();
            }
        }
        else {
            if (componentMap.size() == totalComponentCount) {
                doneResult();
            }
            else if (componentMap.size() > totalComponentCount) {
                String keys = componentMap.keySet().stream().map(Object::toString).collect(Collectors.joining("  \n"));
                log.warn("BUG: Too many operators for %s. Expecting %d; see %d.\n  %s", stateId, totalComponentCount, componentMap.size(), keys);
                snapshotResult = SnapshotResult.FAILED;
                doneResult();
            }
        }

        return true;
    }

    private synchronized void doneResult()
    {
        if (snapshotResult == SnapshotResult.IN_PROGRESS) {
            snapshotResult = SnapshotResult.SUCCESSFUL;
        }
        else if (snapshotResult == SnapshotResult.IN_PROGRESS_FAILED) {
            snapshotResult = SnapshotResult.FAILED;
        }
        else if (snapshotResult == SnapshotResult.IN_PROGRESS_FAILED_FATAL) {
            snapshotResult = SnapshotResult.FAILED_FATAL;
        }
    }

    public SnapshotResult getSnapshotResult()
    {
        return snapshotResult;
    }

    enum ComponentState
    {
        // ordinal is important here.
        SUCCESSFUL,
        FAILED,
        FAILED_FATAL
    }
}
