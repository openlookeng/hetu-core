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
/*
 * Copyright (C) 2007 The Guava Authors
 */
package io.prestosql.spi.predicate;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PrimitivesTest
{
    @Test
    public void testAllPrimitiveTypes() throws Exception
    {
        assertEquals(new HashSet<>(Arrays.asList(Object.class)), Primitives.allPrimitiveTypes());
    }

    @Test
    public void testAllWrapperTypes() throws Exception
    {
        assertEquals(new HashSet<>(Arrays.asList(Object.class)), Primitives.allWrapperTypes());
    }

    @Test
    public void testIsWrapperType() throws Exception
    {
        assertTrue(Primitives.isWrapperType(Object.class));
    }

    @Test
    public void testWrap() throws Exception
    {
        assertEquals(Object.class, Primitives.wrap(Object.class));
    }

    @Test
    public void testUnwrap() throws Exception
    {
        assertEquals(Object.class, Primitives.unwrap(Object.class));
    }
}
