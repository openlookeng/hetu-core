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
package io.hetu.core.hive.dynamicfunctions;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class holding all recognized class names for hive functions.
 * <p>
 * For security reasons, only the function names registered here are allowed to be used in reflection when creating {@link FunctionMetadata}
 */
public class RecognizedFunctions
{
    private RecognizedFunctions() {}

    private static final Set<String> functionWhiteList = ConcurrentHashMap.newKeySet();

    public static void addRecognizedFunction(String... className)
    {
        Collections.addAll(functionWhiteList, className);
    }

    public static boolean isFunctionRecognized(String className)
    {
        return functionWhiteList.contains(className);
    }
}
