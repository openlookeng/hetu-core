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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Calendar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class SecureObjectInputStreamTest
{
    private SecureObjectInputStream secureObjectInputStreamUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        Object o = new Object();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject("content");
        byte[] bytes = byteArrayOutputStream.toByteArray();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);

        secureObjectInputStreamUnderTest = new SecureObjectInputStream(byteArrayInputStream,
                "acceptedClasses");
    }

    @Test
    public void testResolveClass() throws Exception
    {
        // Setup
        final ObjectStreamClass desc = ObjectStreamClass.lookup(Calendar.class);

        // Run the test
        final Class<?> result = secureObjectInputStreamUnderTest.resolveClass(desc);

        // Verify the results
        assertEquals(Object.class, result);
    }

    @Test
    public void testResolveClass_ThrowsIOException()
    {
        // Setup
        final ObjectStreamClass desc = ObjectStreamClass.lookup(Calendar.class);

        // Run the test
        assertThrows(IOException.class, () -> secureObjectInputStreamUnderTest.resolveClass(desc));
    }
}
