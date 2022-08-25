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
package io.prestosql.plugin.base.util;

import org.testng.annotations.Test;

public class ClosablesTest
{
    @Test
    public void testCloseAllSuppress()
    {
        //s(T rootCause, AutoCloseable... closeables)
        // Run the test
        AutoCloseable autoCloseable = new AutoCloseable()
        {
            @Override
            public void close() throws Exception
            {
            }
        };
        Closables.closeAllSuppress(new Throwable(), autoCloseable);

        // Verify the results
    }

    @Test
    public void testCloseAllSuppress1()
    {
        //s(T rootCause, AutoCloseable... closeables)
        // Run the test
        AutoCloseable autoCloseable = new AutoCloseable()
        {
            @Override
            public void close() throws Exception
            {
                close();
            }
        };
        Closables.closeAllSuppress(new Throwable(), autoCloseable);

        // Verify the results
    }
}
