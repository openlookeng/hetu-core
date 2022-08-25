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
package io.hetu.core.common.util;

import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class DataSizeOfUtilTest
{
    @Test
    public void testOf() throws Exception
    {
        // Setup
        DataSizeOfUtil dataSizeOfUtil = new DataSizeOfUtil(0.0, DataSize.Unit.BYTE);
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = DataSizeOfUtil.of(0L, DataSize.Unit.BYTE);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testOf1() throws Exception
    {
        // Setup
        DataSizeOfUtil dataSizeOfUtil = new DataSizeOfUtil(0.0, DataSize.Unit.BYTE);
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Verify the results
        assertThrows(IOException.class, () -> DataSizeOfUtil.of(-1L, DataSize.Unit.BYTE));
    }

    @Test
    public void testOf2() throws Exception
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.MEGABYTE);

        // Run the test
        final DataSize result = DataSizeOfUtil.of(0L, DataSize.Unit.PETABYTE);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
