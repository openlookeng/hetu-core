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
package io.prestosql.spi.predicate;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RangeTest
{
    private Range rangeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        rangeUnderTest = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
    }

    @Test
    public void testGetLowBoundedValue() throws Exception
    {
        // Setup
        // Run the test
        final Object result = rangeUnderTest.getLowBoundedValue();

        // Verify the results
    }

    @Test
    public void testGetHighBoundedValue() throws Exception
    {
        // Setup
        // Run the test
        final Object result = rangeUnderTest.getHighBoundedValue();

        // Verify the results
    }

    @Test
    public void testIsLowUnbounded() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = rangeUnderTest.isLowUnbounded();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsHighUnbounded() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = rangeUnderTest.isHighUnbounded();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetType() throws Exception
    {
        // Setup
        // Run the test
        final Type result = rangeUnderTest.getType();

        // Verify the results
    }

    @Test
    public void testIsSingleValue() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = rangeUnderTest.isSingleValue();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetSingleValue() throws Exception
    {
        // Setup
        // Run the test
        final Object result = rangeUnderTest.getSingleValue();

        // Verify the results
    }

    @Test
    public void testIsAll() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = rangeUnderTest.isAll();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIncludes() throws Exception
    {
        // Setup
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);

        // Run the test
        final boolean result = rangeUnderTest.includes(marker);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testContains() throws Exception
    {
        // Setup
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));

        // Run the test
        final boolean result = rangeUnderTest.contains(other);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testSpan() throws Exception
    {
        // Setup
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        final Range expectedResult = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));

        // Run the test
        final Range result = rangeUnderTest.span(other);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testOverlaps() throws Exception
    {
        // Setup
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));

        // Run the test
        final boolean result = rangeUnderTest.overlaps(other);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIntersect() throws Exception
    {
        // Setup
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        final Range expectedResult = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));

        // Run the test
        final Range result = rangeUnderTest.intersect(other);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, rangeUnderTest.hashCode());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(rangeUnderTest.equals("obj"));
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        final String result = rangeUnderTest.toString(session);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testAll() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.all(type);
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }

    @Test
    public void testGreaterThan() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.greaterThan(type, "low");
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }

    @Test
    public void testGreaterThanOrEqual() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.greaterThanOrEqual(type, "low");
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }

    @Test
    public void testLessThan() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.lessThan(type, "high");
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }

    @Test
    public void testLessThanOrEqual() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.lessThanOrEqual(type, "high");
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }

    @Test
    public void testEqual() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.equal(type, "value");
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }

    @Test
    public void testRange() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final Range result = Range.range(type, "low", false, "high", false);
        assertEquals("result", result.getLowBoundedValue());
        assertEquals("result", result.getHighBoundedValue());
        assertTrue(result.isLowUnbounded());
        assertTrue(result.isHighUnbounded());
        assertEquals(null, result.getType());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getLow());
        assertEquals(new Marker(null, Optional.empty(), Marker.Bound.BELOW), result.getHigh());
        assertTrue(result.isSingleValue());
        assertEquals("result", result.getSingleValue());
        assertTrue(result.isAll());
        final Marker marker = new Marker(null, Optional.empty(), Marker.Bound.BELOW);
        assertTrue(result.includes(marker));
        final Range other = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.contains(other));
        final Range other1 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.span(other1));
        final Range other2 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertTrue(result.overlaps(other2));
        final Range other3 = new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW));
        assertEquals(new Range(new Marker(null, Optional.empty(), Marker.Bound.BELOW),
                new Marker(null, Optional.empty(), Marker.Bound.BELOW)), result.intersect(other3));
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        final ConnectorSession session = null;
        assertEquals("result", result.toString(session));
        assertTrue(result.isLowInclusive());
        assertTrue(result.isHighInclusive());
    }
}
