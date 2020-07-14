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

/**
 * DynamicFilterUtils contains global Dynamic Filter configurations and helper functions
 * @since 2020-04-02
 */
public class DynamicFilterUtils
{
    public static final String REGISTERPREFIX = "register-";
    public static final String FILTERPREFIX = "filter-";
    public static final String FINISHREFIX = "finish-";
    public static final String PARTIALPREFIX = "partial-";
    public static final String WORKERSPREFIX = "workers-";
    public static final String MERGEMAP = "merged";
    public static final String TYPEPREFIX = "type-";
    public static final String HASHSETTYPELOCAL = "HASHSETTYPELOCAL";
    public static final String BLOOMFILTERTYPELOCAL = "BLOOMFILTERTYPELOCAL";
    public static final String HASHSETTYPEGLOBAL = "HASHSETTYPEGLOBAL";
    public static final String BLOOMFILTERTYPEGLOBAL = "BLOOMFILTERTYPEGLOBAL";
    public static final String DFTYPEMAP = "dftypemap";
    public static final double BLOOM_FILTER_EXPECTED_FPP = 0.25F;

    private DynamicFilterUtils()
    {
    }

    public static String createKey(String prefix, String key)
    {
        return prefix + "-" + key;
    }

    public static String createKey(String prefix, String filterKey, String queryId)
    {
        return prefix + filterKey + "-" + queryId;
    }
}
