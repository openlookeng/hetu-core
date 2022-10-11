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
package io.prestosql.plugin.hive.metastore;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.OptionalLong;

public class BooleanStatisticsTest
{
    private BooleanStatistics booleanStatisticsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        booleanStatisticsUnderTest = new BooleanStatistics(OptionalLong.of(0), OptionalLong.of(0));
    }

    @Test
    public void testEquals() throws Exception
    {
        booleanStatisticsUnderTest.equals("o");
    }

    @Test
    public void testHashCode() throws Exception
    {
        booleanStatisticsUnderTest.hashCode();
    }

    @Test
    public void testToString() throws Exception
    {
        booleanStatisticsUnderTest.toString();
    }
}
