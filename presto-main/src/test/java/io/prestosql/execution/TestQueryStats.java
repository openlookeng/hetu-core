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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.operator.FilterAndProjectOperator;
import io.prestosql.operator.OperatorStats;
import io.prestosql.operator.TableWriterOperator;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.plan.PlanNodeId;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestQueryStats
{
    public static final List<OperatorStats> operatorSummaries = ImmutableList.of(
            new OperatorStats(
                    10,
                    11,
                    12,
                    new PlanNodeId("13"),
                    TableWriterOperator.class.getSimpleName(),
                    14L,
                    15L,
                    new Duration(16, NANOSECONDS),
                    new Duration(17, NANOSECONDS),
                    succinctBytes(181L),
                    1811,
                    succinctBytes(182L),
                    1822,
                    succinctBytes(18L),
                    succinctBytes(19L),
                    110L,
                    111.0,
                    112L,
                    new Duration(113, NANOSECONDS),
                    new Duration(114, NANOSECONDS),
                    succinctBytes(116L),
                    117L,
                    succinctBytes(118L),
                    new Duration(119, NANOSECONDS),
                    120L,
                    new Duration(121, NANOSECONDS),
                    new Duration(122, NANOSECONDS),
                    succinctBytes(124L),
                    succinctBytes(125L),
                    succinctBytes(126L),
                    succinctBytes(127L),
                    succinctBytes(128L),
                    succinctBytes(129L),
                    succinctBytes(130L),
                    succinctBytes(131L),
                    new Duration(133, NANOSECONDS),
                    new Duration(134, NANOSECONDS),
                    Optional.empty(),
                    null),
            new OperatorStats(
                    20,
                    21,
                    22,
                    new PlanNodeId("23"),
                    FilterAndProjectOperator.class.getSimpleName(),
                    24L,
                    25L,
                    new Duration(26, NANOSECONDS),
                    new Duration(27, NANOSECONDS),
                    succinctBytes(281L),
                    2811,
                    succinctBytes(282L),
                    2822,
                    succinctBytes(28L),
                    succinctBytes(29L),
                    210L,
                    211.0,
                    212L,
                    new Duration(213, NANOSECONDS),
                    new Duration(214, NANOSECONDS),
                    succinctBytes(216L),
                    217L,
                    succinctBytes(218L),
                    new Duration(219, NANOSECONDS),
                    220L,
                    new Duration(221, NANOSECONDS),
                    new Duration(222, NANOSECONDS),
                    succinctBytes(224L),
                    succinctBytes(225L),
                    succinctBytes(226L),
                    succinctBytes(227L),
                    succinctBytes(228L),
                    succinctBytes(229L),
                    succinctBytes(230L),
                    succinctBytes(231L),
                    new Duration(233, NANOSECONDS),
                    new Duration(234, NANOSECONDS),
                    Optional.empty(),
                    null),
            new OperatorStats(
                    30,
                    31,
                    32,
                    new PlanNodeId("33"),
                    TableWriterOperator.class.getSimpleName(),
                    34L,
                    35L,
                    new Duration(36, NANOSECONDS),
                    new Duration(37, NANOSECONDS),
                    succinctBytes(381L),
                    3811,
                    succinctBytes(382L),
                    3822,
                    succinctBytes(38L),
                    succinctBytes(39L),
                    310L,
                    311.0,
                    312L,
                    new Duration(313, NANOSECONDS),
                    new Duration(314, NANOSECONDS),
                    succinctBytes(316L),
                    317L,
                    succinctBytes(318L),
                    new Duration(319, NANOSECONDS),
                    320L,
                    new Duration(321, NANOSECONDS),
                    new Duration(322, NANOSECONDS),
                    succinctBytes(324L),
                    succinctBytes(325L),
                    succinctBytes(326L),
                    succinctBytes(327L),
                    succinctBytes(328L),
                    succinctBytes(329L),
                    succinctBytes(330L),
                    succinctBytes(331L),
                    new Duration(333, NANOSECONDS),
                    new Duration(334, NANOSECONDS),
                    Optional.empty(),
                    null));

    public static final QueryStats EXPECTED = new QueryStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),
            new DateTime(4),
            new Duration(6, NANOSECONDS),
            new Duration(5, NANOSECONDS),
            new Duration(31, NANOSECONDS),
            new Duration(32, NANOSECONDS),
            new Duration(41, NANOSECONDS),
            new Duration(7, NANOSECONDS),
            new Duration(8, NANOSECONDS),

            new Duration(100, NANOSECONDS),
            new Duration(100, NANOSECONDS),
            new Duration(100, NANOSECONDS),
            new Duration(200, NANOSECONDS),

            9,
            10,
            11,

            12,
            13,
            15,
            30,
            16,

            17.0,
            new DataSize(18, BYTE),
            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),
            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            new DataSize(25, BYTE),
            new DataSize(26, BYTE),

            true,
            new Duration(20, NANOSECONDS),
            new Duration(21, NANOSECONDS),
            new Duration(23, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(241, BYTE),
            251,

            new DataSize(242, BYTE),
            252,

            new DataSize(24, BYTE),
            25,

            new DataSize(26, BYTE),
            27,

            new DataSize(28, BYTE),
            29,

            new DataSize(30, BYTE),

            ImmutableList.of(new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107)),

            operatorSummaries);

    @Test
    public void testJson()
    {
        JsonCodec<QueryStats> codec = JsonCodec.jsonCodec(QueryStats.class);

        String json = codec.toJson(EXPECTED);
        QueryStats actual = codec.fromJson(json);

        assertExpectedQueryStats(actual);
    }

    public static void assertExpectedQueryStats(QueryStats actual)
    {
        assertEquals(actual.getCreateTime(), new DateTime(1, UTC));
        assertEquals(actual.getExecutionStartTime(), new DateTime(2, UTC));
        assertEquals(actual.getLastHeartbeat(), new DateTime(3, UTC));
        assertEquals(actual.getEndTime(), new DateTime(4, UTC));

        assertEquals(actual.getElapsedTime(), new Duration(6, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getResourceWaitingTime(), new Duration(31, NANOSECONDS));
        assertEquals(actual.getDispatchingTime(), new Duration(32, NANOSECONDS));
        assertEquals(actual.getExecutionTime(), new Duration(41, NANOSECONDS));
        assertEquals(actual.getAnalysisTime(), new Duration(7, NANOSECONDS));
        assertEquals(actual.getDistributedPlanningTime(), new Duration(8, NANOSECONDS));

        assertEquals(actual.getTotalPlanningTime(), new Duration(100, NANOSECONDS));
        assertEquals(actual.getFinishingTime(), new Duration(200, NANOSECONDS));

        assertEquals(actual.getTotalTasks(), 9);
        assertEquals(actual.getRunningTasks(), 10);
        assertEquals(actual.getCompletedTasks(), 11);

        assertEquals(actual.getTotalDrivers(), 12);
        assertEquals(actual.getQueuedDrivers(), 13);
        assertEquals(actual.getRunningDrivers(), 15);
        assertEquals(actual.getBlockedDrivers(), 30);
        assertEquals(actual.getCompletedDrivers(), 16);

        assertEquals(actual.getCumulativeUserMemory(), 17.0);
        assertEquals(actual.getUserMemoryReservation(), new DataSize(18, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(19, BYTE));
        assertEquals(actual.getTotalMemoryReservation(), new DataSize(20, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(21, BYTE));
        assertEquals(actual.getPeakRevocableMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakTaskUserMemory(), new DataSize(24, BYTE));
        assertEquals(actual.getPeakTaskRevocableMemory(), new DataSize(25, BYTE));
        assertEquals(actual.getPeakTaskTotalMemory(), new DataSize(26, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(693, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(20, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(21, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(23, NANOSECONDS));

        assertEquals(actual.getPhysicalInputDataSize(), new DataSize(241, BYTE));
        assertEquals(actual.getPhysicalInputPositions(), 251);

        assertEquals(actual.getInternalNetworkInputDataSize(), new DataSize(242, BYTE));
        assertEquals(actual.getInternalNetworkInputPositions(), 252);

        assertEquals(actual.getRawInputDataSize(), new DataSize(24, BYTE));
        assertEquals(actual.getRawInputPositions(), 25);

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(26, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 27);

        assertEquals(actual.getOutputDataSize(), new DataSize(28, BYTE));
        assertEquals(actual.getOutputPositions(), 29);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(30, BYTE));

        assertEquals(actual.getStageGcStatistics().size(), 1);
        StageGcStatistics gcStatistics = actual.getStageGcStatistics().get(0);
        assertEquals(gcStatistics.getStageId(), 101);
        assertEquals(gcStatistics.getTasks(), 102);
        assertEquals(gcStatistics.getFullGcTasks(), 103);
        assertEquals(gcStatistics.getMinFullGcSec(), 104);
        assertEquals(gcStatistics.getMaxFullGcSec(), 105);
        assertEquals(gcStatistics.getTotalFullGcSec(), 106);
        assertEquals(gcStatistics.getAverageFullGcSec(), 107);

        assertEquals(420, actual.getWrittenPositions());
        assertEquals(58, actual.getLogicalWrittenDataSize().toBytes());
    }
}
