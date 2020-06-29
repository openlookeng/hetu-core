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
package io.hetu.core.hive;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHiveFunctionsPlugin
{
    private final ClassLoader classLoader = this.getClass().getClassLoader();

    @Test
    public void testGetDynamicHiveFunctions()
    {
        String propPath = classLoader.getResource("").getPath() + "udf.properties";
        HiveFunctionsPlugin plugin = new HiveFunctionsPlugin(propPath, classLoader);
        assertEquals(4, plugin.getDynamicHiveFunctions().size());
    }

    @Test
    public void testGetDynamicHiveFunctionsWithNonExistingFunctions()
    {
        String propPath = classLoader.getResource("").getPath() + "udf_with_non_existing_function.properties";
        HiveFunctionsPlugin plugin = new HiveFunctionsPlugin(propPath, classLoader);
        assertEquals(4, plugin.getDynamicHiveFunctions().size());
    }

    @Test
    public void testGetDynamicHiveFunctionsWIthNonExistingPropertiesFile()
    {
        String propPath = classLoader.getResource("").getPath() + "udf_non_existing.properties";
        HiveFunctionsPlugin plugin = new HiveFunctionsPlugin(propPath, classLoader);
        assertEquals(0, plugin.getDynamicHiveFunctions().size());
    }

    @Test
    public void testGetDynamicHiveFunctionsWithDuplicatedFunctions()
    {
        String propPath = classLoader.getResource("").getPath() + "udf_with_duplicated_functions.properties";
        HiveFunctionsPlugin plugin = new HiveFunctionsPlugin(propPath, classLoader);
        assertEquals(4, plugin.getDynamicHiveFunctions().size());
    }
}
