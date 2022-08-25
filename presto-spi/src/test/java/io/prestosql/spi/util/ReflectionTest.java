/*
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
package io.prestosql.spi.util;

import io.prestosql.spi.StandardErrorCode;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.testng.Assert.assertEquals;

public class ReflectionTest
{
    @Test
    public void testField() throws Exception
    {
        // Setup
        final Field expectedResult = null;

        // Run the test
        final Field result = Reflection.field(Object.class, "name");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMethod() throws Exception
    {
        // Setup
        final Method expectedResult = null;

        // Run the test
        final Method result = Reflection.method(Object.class, "name", Object.class);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMethodHandle1()
    {
        // Setup
        // Run the test
        final MethodHandle result = Reflection.methodHandle(Object.class, "name", Object.class);

        // Verify the results
    }

    @Test
    public void testMethodHandle2() throws Exception
    {
        // Setup
        final Method method = null;

        // Run the test
        final MethodHandle result = Reflection.methodHandle(StandardErrorCode.TABLE_NOT_FOUND, method);

        // Verify the results
    }

    @Test
    public void testMethodHandle3() throws Exception
    {
        // Setup
        final Method method = null;

        // Run the test
        final MethodHandle result = Reflection.methodHandle(method);

        // Verify the results
    }

    @Test
    public void testConstructorMethodHandle1() throws Exception
    {
        // Setup
        // Run the test
        final MethodHandle result = Reflection.constructorMethodHandle(Object.class, Object.class);

        // Verify the results
    }

    @Test
    public void testConstructorMethodHandle2() throws Exception
    {
        // Setup
        // Run the test
        final MethodHandle result = Reflection.constructorMethodHandle(StandardErrorCode.TABLE_NOT_FOUND, Object.class,
                Object.class);

        // Verify the results
    }

    @Test
    public void testConstructorMethodHandle3() throws Exception
    {
        // Setup
        final Constructor<?> constructor = null;

        // Run the test
        final MethodHandle result = Reflection.constructorMethodHandle(StandardErrorCode.TABLE_NOT_FOUND, constructor);

        // Verify the results
    }
}
