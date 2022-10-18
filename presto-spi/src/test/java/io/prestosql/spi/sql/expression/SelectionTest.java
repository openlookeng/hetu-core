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
package io.prestosql.spi.sql.expression;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SelectionTest
{
    private Selection selectionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        selectionUnderTest = new Selection("alias", "alias");
    }

    @Test
    public void testIsAliased() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = selectionUnderTest.isAliased(false);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", selectionUnderTest.toString());
    }
}
