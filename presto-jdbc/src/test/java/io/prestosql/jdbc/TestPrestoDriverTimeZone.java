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
package io.prestosql.jdbc;

import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.prestosql.plugin.blackhole.BlackHolePlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Float.POSITIVE_INFINITY;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoDriverTimeZone
{
    private static final DateTimeZone ASIA_SHANGHAI = DateTimeZone.forID("Asia/Shanghai");
    private static final GregorianCalendar ASIA_SHANGHAI_CALENDAR = new GregorianCalendar(ASIA_SHANGHAI.toTimeZone());
    private static final DateTimeZone EUROPE_BERLIN = DateTimeZone.forID("Europe/Berlin");
    private static final GregorianCalendar EUROPE_BERLIN_CALENDAR = new GregorianCalendar(EUROPE_BERLIN.toTimeZone());
    private static final DateTimeZone UTC_ZONE = DateTimeZone.forID("UTC");
    private static final GregorianCalendar UTC_CALENDAR = new GregorianCalendar(UTC_ZONE.toTimeZone());
    private static final String TEST_CATALOG = "test_catalog";

    private TestingPrestoServer server;
    private ExecutorService executorService;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        waitForNodeRefresh(server);
        setupTestTables();
        executorService = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    private static void waitForNodeRefresh(TestingPrestoServer server)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (server.refreshNodes().getActiveNodes().size() < 1) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }

    private void setupTestTables()
            throws SQLException
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate("CREATE SCHEMA blackhole.blackhole"), 0);
            assertEquals(statement.executeUpdate("CREATE TABLE test_table (x bigint)"), 0);

            assertEquals(statement.executeUpdate("CREATE TABLE slow_test_table (x bigint) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")"), 0);
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        executorService.shutdownNow();
    }

    @Test
    public void testTypes()
            throws Exception
    {
        DateTimeZone localTimeZone = DateTimeZone.forTimeZone(ASIA_SHANGHAI_CALENDAR.getTimeZone());
        LocalDateTime localDateTime = ISODateTimeFormat.date().withZone(localTimeZone).parseLocalDateTime(String.valueOf("1940-06-03"));
        long timeInMillis = localDateTime.toDate(localTimeZone.toTimeZone()).getTime();

        System.out.println(new Date(timeInMillis));

        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT " +
                        "  TIME '3:04:05' as a" +
                        ", TIME '6:07:08 +06:17' as b" +
                        ", TIME '9:10:11 Europe/Berlin' as c" +
                        ", TIMESTAMP '2001-02-03 3:04:05' as d" +
                        ", TIMESTAMP '2004-05-06 6:07:08 +06:17' as e" +
                        ", TIMESTAMP '2007-08-09 9:10:11 Europe/Berlin' as f" +
                        ", DATE '1940-06-03' as g" +
                        ", INTERVAL '123-11' YEAR TO MONTH as h" +
                        ", INTERVAL '11 22:33:44.555' DAY TO SECOND as i" +
                        ", REAL '123.45' as j" +
                        ", REAL 'Infinity' as k" +
                        ", TIMESTAMP '2018-03-25 2:10:10' as l" +
                        ", TIMESTAMP '2018-03-25 2:10:10 UTC' as m" +
                        ", TIME '14:04:05 Europe/Berlin' as n" +
                        ", TIME '2:04:05 Europe/Berlin' as o" +
                        "")) {
                    assertTrue(rs.next());

                    assertEquals(rs.getTime(1), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime(1, ASIA_SHANGHAI_CALENDAR), new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_SHANGHAI).getMillis()));
                    assertEquals(rs.getObject(1), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("a"), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("a", ASIA_SHANGHAI_CALENDAR), new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_SHANGHAI).getMillis()));
                    assertEquals(rs.getObject("a"), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));

                    assertEquals(rs.getTime(2), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime(2, ASIA_SHANGHAI_CALENDAR), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(2), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime("b"), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime("b", ASIA_SHANGHAI_CALENDAR), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("b"), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));

                    assertEquals(rs.getTime(3), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime(3, ASIA_SHANGHAI_CALENDAR), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject(3), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime("c"), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime("c", ASIA_SHANGHAI_CALENDAR), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject("c"), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));

                    assertEquals(rs.getTimestamp(4), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp(4, ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_SHANGHAI).getMillis()));
                    assertEquals(rs.getObject(4), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp("d"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp("d", ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_SHANGHAI).getMillis()));
                    assertEquals(rs.getObject("d"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("n"), new Time(new DateTime(1970, 1, 1, 14, 4, 5, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime("o"), new Time(new DateTime(1970, 1, 1, 2, 4, 5, DateTimeZone.forID("Europe/Berlin")).getMillis()));

                    assertEquals(rs.getTimestamp(5), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp(5, ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(5), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp("e"), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp("e", ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("e"), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));

                    assertEquals(rs.getTimestamp(6), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp(6, ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject(6), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp("f"), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp("f", ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject("f"), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));

                    assertEquals(rs.getTimestamp("l", EUROPE_BERLIN_CALENDAR), new Timestamp(new DateTime(2018, 3, 25, 3, 10, 10, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp("m"), new Timestamp(new DateTime(2018, 3, 25, 2, 10, 10, DateTimeZone.forID("UTC")).getMillis()));
                    assertEquals(rs.getTimestamp("m", ASIA_SHANGHAI_CALENDAR), new Timestamp(new DateTime(2018, 3, 25, 2, 10, 10, DateTimeZone.forID("UTC")).getMillis()));
                    assertEquals(rs.getTimestamp("m", EUROPE_BERLIN_CALENDAR), new Timestamp(new DateTime(2018, 3, 25, 2, 10, 10, DateTimeZone.forID("UTC")).getMillis()));
                    assertEquals(rs.getTimestamp("m", UTC_CALENDAR), new Timestamp(new DateTime(2018, 3, 25, 2, 10, 10, DateTimeZone.forID("UTC")).getMillis()));

                    assertEquals(rs.getDate(7), new Date(new DateTime(1940, 6, 3, 0, 0).getMillis()));
                    assertEquals(rs.getDate(7, ASIA_SHANGHAI_CALENDAR).toLocalDate(), new Date(1940 - 1900, 5, 2).toLocalDate());
                    assertEquals(rs.getObject(7), new Date(new DateTime(1940, 6, 3, 0, 0).getMillis()));
                    assertEquals(rs.getDate("g"), new Date(new DateTime(1940, 6, 3, 0, 0).getMillis()));
                    assertEquals(rs.getDate("g", ASIA_SHANGHAI_CALENDAR).toLocalDate(), new Date(1940 - 1900, 5, 2).toLocalDate());
                    assertEquals(rs.getObject("g"), new Date(new DateTime(1940, 6, 3, 0, 0).getMillis()));

                    assertEquals(rs.getObject(8), new PrestoIntervalYearMonth(123, 11));
                    assertEquals(rs.getObject("h"), new PrestoIntervalYearMonth(123, 11));
                    assertEquals(rs.getObject(9), new PrestoIntervalDayTime(11, 22, 33, 44, 555));
                    assertEquals(rs.getObject("i"), new PrestoIntervalDayTime(11, 22, 33, 44, 555));

                    assertEquals(rs.getFloat(10), 123.45f);
                    assertEquals(rs.getObject(10), 123.45f);
                    assertEquals(rs.getFloat("j"), 123.45f);
                    assertEquals(rs.getObject("j"), 123.45f);

                    assertEquals(rs.getFloat(11), POSITIVE_INFINITY);
                    assertEquals(rs.getObject(11), POSITIVE_INFINITY);
                    assertEquals(rs.getFloat("k"), POSITIVE_INFINITY);
                    assertEquals(rs.getObject("k"), POSITIVE_INFINITY);
                    assertFalse(rs.next());
                }
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }
}
