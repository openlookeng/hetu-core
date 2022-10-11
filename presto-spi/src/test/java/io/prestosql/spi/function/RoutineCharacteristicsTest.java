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
package io.prestosql.spi.function;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RoutineCharacteristicsTest
{
    private RoutineCharacteristics routineCharacteristicsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        routineCharacteristicsUnderTest = new RoutineCharacteristics(
                Optional.of(new RoutineCharacteristics.Language("language")),
                Optional.of(RoutineCharacteristics.Determinism.DETERMINISTIC),
                Optional.of(RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT));
    }

    @Test
    public void testIsDeterministic() throws Exception
    {
        assertTrue(routineCharacteristicsUnderTest.isDeterministic());
    }

    @Test
    public void testIsCalledOnNullInput() throws Exception
    {
        assertTrue(routineCharacteristicsUnderTest.isCalledOnNullInput());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(routineCharacteristicsUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, routineCharacteristicsUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", routineCharacteristicsUnderTest.toString());
    }

    @Test
    public void testBuilder1() throws Exception
    {
        // Setup
        // Run the test
        final RoutineCharacteristics.Builder result = RoutineCharacteristics.builder();

        // Verify the results
    }

    @Test
    public void testBuilder2() throws Exception
    {
        // Setup
        final RoutineCharacteristics routineCharacteristics = new RoutineCharacteristics(
                Optional.of(new RoutineCharacteristics.Language("language")),
                Optional.of(RoutineCharacteristics.Determinism.DETERMINISTIC),
                Optional.of(RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT));

        // Run the test
        final RoutineCharacteristics.Builder result = RoutineCharacteristics.builder(routineCharacteristics);

        // Verify the results
    }
}
