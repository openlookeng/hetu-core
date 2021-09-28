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

public enum SnapshotResult
{
    // For both capture and restore
    SUCCESSFUL(true),
    FAILED(true),
    IN_PROGRESS(false),
    IN_PROGRESS_FAILED(false),
    // For restore only
    FAILED_FATAL(true),
    IN_PROGRESS_FAILED_FATAL(false),
    // The snapshot is unusable, as if it didn't exist. Different from failed ones, in that
    // they are skipped over during "backtrack", whereas failed entries stops the traversal.
    NA(true);

    private final boolean doneResult;

    SnapshotResult(boolean doneResult)
    {
        this.doneResult = doneResult;
    }

    public boolean isDone()
    {
        return doneResult;
    }
}
