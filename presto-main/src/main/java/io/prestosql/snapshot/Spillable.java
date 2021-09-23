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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Indicate whether an object support spill to disk
 */
public interface Spillable
{
    /**
     * Determine if an object is spilled.
     *
     * @return true if enabled
     */
    default boolean isSpilled()
    {
        return false;
    }

    /**
     * Get spilled file paths of an object
     *
     * @return spilled file paths
     */
    default List<Path> getSpilledFilePaths()
    {
        return new ArrayList<>();
    }
}
