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
package io.prestosql.spi.heuristicindex;

import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

public class TypeUtilsTest
{
    @Test
    public void testGetActualValue() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Object result = TypeUtils.getActualValue(type, "value");

        // Verify the results
    }

    @Test
    public void testExtractValueFromRowExpression()
    {
        // Setup
        final RowExpression rowExpression = null;

        // Run the test
        final Object result = TypeUtils.extractValueFromRowExpression(rowExpression);

        // Verify the results
    }
}
