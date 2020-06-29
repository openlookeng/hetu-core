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

package io.hetu.core.heuristicindex.util;

/**
 * Global constants used in hetu-heuristic-index
 */
public class ConstantHelper
{
    /**
     * The lastModified file prefix.
     * lastModified file is created under the same directory of the index file so that the coordinator knows
     * whether the index file is out of date.
     */
    public static final String LAST_MODIFIED_FILE_PREFIX = "lastModified=";

    private ConstantHelper()
    {
    }
}
