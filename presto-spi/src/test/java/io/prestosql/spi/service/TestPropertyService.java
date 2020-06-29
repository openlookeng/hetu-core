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
package io.prestosql.spi.service;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPropertyService
{
    @Test
    public void testGetStringProperty()
    {
        String testStringProperty = "test.string.property";
        String testString = "true";
        PropertyService.setProperty(testStringProperty, testString);
        assertEquals(PropertyService.getStringProperty(testStringProperty), testString);
    }

    @Test
    public void testGetBooleanProperty()
    {
        String testBooleanProperty = "test.boolean.property";
        PropertyService.setProperty(testBooleanProperty, true);
        assertTrue(PropertyService.getBooleanProperty(testBooleanProperty));
    }

    @Test
    public void testGetLongProperty()
    {
        String testLongProperty = "test.long.property";
        Long testLong = Long.valueOf(123);
        PropertyService.setProperty(testLongProperty, testLong);
        assertEquals(PropertyService.getLongProperty(testLongProperty), testLong);
    }

    @Test
    public void testGetDoubleProperty()
    {
        String testDoubleProperty = "test.double.property";
        Double testDouble = Double.valueOf(123);
        PropertyService.setProperty(testDoubleProperty, testDouble);
        assertEquals(PropertyService.getDoubleProperty(testDoubleProperty), testDouble);
    }
}
