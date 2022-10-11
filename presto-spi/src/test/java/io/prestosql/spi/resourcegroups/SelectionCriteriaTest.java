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
package io.prestosql.spi.resourcegroups;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.session.ResourceEstimates;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class SelectionCriteriaTest
{
    private SelectionCriteria selectionCriteriaUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        selectionCriteriaUnderTest = new SelectionCriteria(false, "user", Optional.of("value"), new HashSet<>(
                Arrays.asList("value")), new ResourceEstimates(
                Optional.of(new Duration(0.0, TimeUnit.MILLISECONDS)),
                Optional.of(new Duration(0.0, TimeUnit.MILLISECONDS)),
                Optional.of(new DataSize(0.0, DataSize.Unit.BYTE))),
                Optional.of("value"));
    }

    @Test
    public void testGetTags() throws Exception
    {
        assertEquals(new HashSet<>(Arrays.asList("value")), selectionCriteriaUnderTest.getTags());
    }
}
