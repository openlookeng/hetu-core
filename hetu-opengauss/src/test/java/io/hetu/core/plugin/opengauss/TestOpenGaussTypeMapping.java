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
package io.hetu.core.plugin.opengauss;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.postgresql.TestPostgreSqlTypeMapping;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.TestingSession;
import io.prestosql.tests.datatype.DataTypeTest;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.tests.datatype.DataType.bigintDataType;
import static io.prestosql.tests.datatype.DataType.booleanDataType;
import static io.prestosql.tests.datatype.DataType.dateDataType;
import static io.prestosql.tests.datatype.DataType.decimalDataType;
import static io.prestosql.tests.datatype.DataType.doubleDataType;
import static io.prestosql.tests.datatype.DataType.integerDataType;
import static io.prestosql.tests.datatype.DataType.realDataType;
import static io.prestosql.tests.datatype.DataType.smallintDataType;
import static io.prestosql.tests.datatype.DataType.timestampDataType;
import static java.util.Arrays.asList;

@Test
public class TestOpenGaussTypeMapping
        extends TestPostgreSqlTypeMapping
{
    public TestOpenGaussTypeMapping()
            throws Exception
    {
        this(new TestOpenGaussServer());
    }

    private TestOpenGaussTypeMapping(TestOpenGaussServer openGaussServer)
    {
        super(() -> OpenGaussQueryRunner.createOpenGaussQueryRunner(
                openGaussServer,
                ImmutableMap.of("postgresql.experimental.array-mapping", "AS_ARRAY"),
                ImmutableList.of()), openGaussServer);
    }

    // not support
    @Test
    public void testPrestoCreatedParameterizedVarcharUnicode()
    {
    }

    // not support
    @Test
    public void testPostgreSqlCreatedParameterizedVarcharUnicode()
    {
    }

    // not support
    @Test
    public void testPrestoCreatedParameterizedCharUnicode()
    {
    }

    // not support
    @Test
    public void testPostgreSqlCreatedParameterizedCharUnicode()
    {
    }

    @Test
    public void testArray()
    {
        // basic types
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(booleanDataType()), asList(true, false))
                .addRoundTrip(arrayDataType(bigintDataType()), asList(123_456_789_012L))
                .addRoundTrip(arrayDataType(integerDataType()), asList(1, 2, 1_234_567_890))
                .addRoundTrip(arrayDataType(smallintDataType()), asList((short) 32_456))
                .addRoundTrip(arrayDataType(doubleDataType()), asList(123.45d))
                .addRoundTrip(arrayDataType(realDataType()), asList(123.45f))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_basic"));

        arrayDecimalTest(TestOpenGaussTypeMapping::arrayDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_decimal"));
        arrayDecimalTest(TestOpenGaussTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_decimal"));

        arrayVarcharDataTypeTest(TestOpenGaussTypeMapping::arrayDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_varchar"));
        arrayVarcharDataTypeTest(TestOpenGaussTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_varchar"));
    }

    @Test
    public void testArrayMultidimensional()
    {
        // for multidimensional arrays, PostgreSQL requires subarrays to have the same dimensions, including nulls
        // e.g. [[1], [1, 2]] and [null, [1, 2]] are not allowed, but [[null, null], [1, 2]] is allowed
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(arrayDataType(booleanDataType())), asList(asList(null, null, null)))
                .addRoundTrip(arrayDataType(arrayDataType(booleanDataType())), asList(asList(true, null), asList(null, null), asList(false, false)))
                .addRoundTrip(arrayDataType(arrayDataType(integerDataType())), asList(asList(1, 2), asList(null, null), asList(3, 4)))
                .addRoundTrip(arrayDataType(arrayDataType(decimalDataType(3, 0))), asList(
                        asList(new BigDecimal("193")),
                        asList(new BigDecimal("19")),
                        asList(new BigDecimal("-193"))))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_2d"));
    }

    @Test
    public void testDate()
    {
        // Note: there is identical test for MySQL

        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
        }
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(boolean insertWithPresto)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        for (ZoneId sessionZone : ImmutableList.of(ZoneOffset.UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            DataTypeTest tests = DataTypeTest.create()
                    .addRoundTrip(timestampDataType(), beforeEpoch)
                    .addRoundTrip(timestampDataType(), afterEpoch)
                    .addRoundTrip(timestampDataType(), timeDoubledInJvmZone)
                    .addRoundTrip(timestampDataType(), timeDoubledInVilnius);

            if (!insertWithPresto) {
                // when writing, Postgres JDBC driver converts LocalDateTime to string representing date-time in JVM zone
                // TODO upgrade driver or find a different way to write timestamp values
                addTimestampTestIfSupported(tests, epoch); // epoch also is a gap in JVM zone
                addTimestampTestIfSupported(tests, timeGapInJvmZone1);
                addTimestampTestIfSupported(tests, timeGapInJvmZone2);
            }

            addTimestampTestIfSupported(tests, timeGapInVilnius);
            addTimestampTestIfSupported(tests, timeGapInKathmandu);

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            if (insertWithPresto) {
                tests.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_timestamp"));
            }
        }
    }

    // not support
    @Test
    public void testJsonb()
    {
    }
}
