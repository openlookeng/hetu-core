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
package io.prestosql.spi.plan;

import com.google.common.base.Predicate;
import io.prestosql.spi.relation.RowExpression;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AssignmentsTest
{
    private Assignments assignmentsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        assignmentsUnderTest = new Assignments(new HashMap<>());
    }

    @Test
    public void testGetOutputs() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = assignmentsUnderTest.getOutputs();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetMap() throws Exception
    {
        // Setup
        final Map<Symbol, RowExpression> expectedResult = new HashMap<>();

        // Run the test
        final Map<Symbol, RowExpression> result = assignmentsUnderTest.getMap();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testFilter1()
    {
        // Setup
        final Collection<Symbol> symbols = Arrays.asList(new Symbol("name"));
        final Assignments expectedResult = new Assignments(new HashMap<>());

        // Run the test
        final Assignments result = assignmentsUnderTest.filter(symbols);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testFilter2() throws Exception
    {
        // Setup
        final Predicate<Symbol> predicate = null;
        final Assignments expectedResult = new Assignments(new HashMap<>());

        // Run the test
        final Assignments result = assignmentsUnderTest.filter(predicate);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetExpressions() throws Exception
    {
        // Setup
        final Collection<RowExpression> expectedResult = Arrays.asList();

        // Run the test
        final Collection<RowExpression> result = assignmentsUnderTest.getExpressions();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetSymbols() throws Exception
    {
        // Setup
        final Set<Symbol> expectedResult = new HashSet<>(Arrays.asList(new Symbol("name")));

        // Run the test
        final Set<Symbol> result = assignmentsUnderTest.getSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEntrySet() throws Exception
    {
        // Setup
        final Set<Map.Entry<Symbol, RowExpression>> expectedResult = new HashSet<>();

        // Run the test
        final Set<Map.Entry<Symbol, RowExpression>> result = assignmentsUnderTest.entrySet();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGet() throws Exception
    {
        // Setup
        final Symbol symbol = new Symbol("name");
        final RowExpression expectedResult = null;

        // Run the test
        final RowExpression result = assignmentsUnderTest.get(symbol);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSize() throws Exception
    {
        // Setup
        // Run the test
        final int result = assignmentsUnderTest.size();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testIsEmpty() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = assignmentsUnderTest.isEmpty();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testForEach() throws Exception
    {
        // Setup
        final BiConsumer<Symbol, RowExpression> mockConsumer = mock(BiConsumer.class);

        // Run the test
        assignmentsUnderTest.forEach(mockConsumer);

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = assignmentsUnderTest.equals("o");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Setup
        // Run the test
        final int result = assignmentsUnderTest.hashCode();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testBuilder1() throws Exception
    {
        // Setup
        // Run the test
        final Assignments.Builder result = Assignments.builder();

        // Verify the results
    }

    @Test
    public void testBuilder2() throws Exception
    {
        // Setup
        final Map<Symbol, RowExpression> assignments = new HashMap<>();

        // Run the test
        final Assignments.Builder result = Assignments.builder(assignments);

        // Verify the results
    }

    @Test
    public void testCopyOf() throws Exception
    {
        // Setup
        final Map<Symbol, RowExpression> assignments = new HashMap<>();

        // Run the test
        final Assignments result = Assignments.copyOf(assignments);
        assertEquals(Arrays.asList(new Symbol("name")), result.getOutputs());
        assertEquals(new HashMap<>(), result.getMap());
        final Collection<Symbol> symbols = Arrays.asList(new Symbol("name"));
        assertEquals(new Assignments(new HashMap<>()), result.filter(symbols));
        final Predicate<Symbol> predicate = null;
        assertEquals(new Assignments(new HashMap<>()), result.filter(predicate));
        assertEquals(Arrays.asList(), result.getExpressions());
        assertEquals(new HashSet<>(Arrays.asList(new Symbol("name"))), result.getSymbols());
        assertEquals(new HashSet<>(), result.entrySet());
        final Symbol symbol = new Symbol("name");
        assertEquals(null, result.get(symbol));
        assertEquals(0, result.size());
        assertTrue(result.isEmpty());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
    }

    @Test
    public void testOf1()
    {
        // Run the test
        final Assignments result = Assignments.of();
        assertEquals(Arrays.asList(new Symbol("name")), result.getOutputs());
        assertEquals(new HashMap<>(), result.getMap());
        final Collection<Symbol> symbols = Arrays.asList(new Symbol("name"));
        assertEquals(new Assignments(new HashMap<>()), result.filter(symbols));
        final Predicate<Symbol> predicate = null;
        assertEquals(new Assignments(new HashMap<>()), result.filter(predicate));
        assertEquals(Arrays.asList(), result.getExpressions());
        assertEquals(new HashSet<>(Arrays.asList(new Symbol("name"))), result.getSymbols());
        assertEquals(new HashSet<>(), result.entrySet());
        final Symbol symbol = new Symbol("name");
        assertEquals(null, result.get(symbol));
        assertEquals(0, result.size());
        assertTrue(result.isEmpty());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
    }

    @Test
    public void testOf2() throws Exception
    {
        // Setup
        final Symbol symbol = new Symbol("name");
        final RowExpression expression = null;

        // Run the test
        final Assignments result = Assignments.of(symbol, expression);
        assertEquals(Arrays.asList(new Symbol("name")), result.getOutputs());
        assertEquals(new HashMap<>(), result.getMap());
        final Collection<Symbol> symbols = Arrays.asList(new Symbol("name"));
        assertEquals(new Assignments(new HashMap<>()), result.filter(symbols));
        final Predicate<Symbol> predicate = null;
        assertEquals(new Assignments(new HashMap<>()), result.filter(predicate));
        assertEquals(Arrays.asList(), result.getExpressions());
        assertEquals(new HashSet<>(Arrays.asList(new Symbol("name"))), result.getSymbols());
        assertEquals(new HashSet<>(), result.entrySet());
        final Symbol symbol1 = new Symbol("name");
        assertEquals(null, result.get(symbol1));
        assertEquals(0, result.size());
        assertTrue(result.isEmpty());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
    }

    @Test
    public void testOf3()
    {
        // Setup
        final Symbol symbol1 = new Symbol("name");
        final RowExpression expression1 = null;
        final Symbol symbol2 = new Symbol("name");
        final RowExpression expression2 = null;

        // Run the test
        final Assignments result = Assignments.of(symbol1, expression1, symbol2, expression2);
        assertEquals(Arrays.asList(new Symbol("name")), result.getOutputs());
        assertEquals(new HashMap<>(), result.getMap());
        final Collection<Symbol> symbols = Arrays.asList(new Symbol("name"));
        assertEquals(new Assignments(new HashMap<>()), result.filter(symbols));
        final Predicate<Symbol> predicate = null;
        assertEquals(new Assignments(new HashMap<>()), result.filter(predicate));
        assertEquals(Arrays.asList(), result.getExpressions());
        assertEquals(new HashSet<>(Arrays.asList(new Symbol("name"))), result.getSymbols());
        assertEquals(new HashSet<>(), result.entrySet());
        final Symbol symbol = new Symbol("name");
        assertEquals(null, result.get(symbol));
        assertEquals(0, result.size());
        assertTrue(result.isEmpty());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
    }
}
