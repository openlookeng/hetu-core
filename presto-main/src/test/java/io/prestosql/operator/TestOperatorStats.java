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
package io.prestosql.operator;

import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.operator.PartitionedOutputOperator.PartitionedOutputInfo;
import io.prestosql.spi.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestOperatorStats
{
    private static final SplitOperatorInfo NON_MERGEABLE_INFO = new SplitOperatorInfo("some_info");
    private static final PartitionedOutputInfo MERGEABLE_INFO = new PartitionedOutputInfo(1, 2, 1024);

    public static final OperatorStats EXPECTED = new OperatorStats(
            0,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            new DataSize(51, BYTE),
            511,
            new DataSize(52, BYTE),
            522,
            new DataSize(5, BYTE),
            new DataSize(6, BYTE),
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            new DataSize(12, BYTE),
            13,

            new DataSize(14, BYTE),

            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),

            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),
            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            new DataSize(25, BYTE),
            new DataSize(26, BYTE),
            new Duration(27, NANOSECONDS),
            new Duration(28, NANOSECONDS),
            Optional.empty(),
            NON_MERGEABLE_INFO);

    public static final OperatorStats MERGEABLE = new OperatorStats(
            0,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            new DataSize(51, BYTE),
            511,
            new DataSize(52, BYTE),
            522,
            new DataSize(5, BYTE),
            new DataSize(6, BYTE),
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            new DataSize(12, BYTE),
            13,

            new DataSize(14, BYTE),

            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),

            new DataSize(19, BYTE),
            new DataSize(20, BYTE),
            new DataSize(21, BYTE),
            new DataSize(22, BYTE),
            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            new DataSize(25, BYTE),
            new DataSize(26, BYTE),
            new Duration(27, NANOSECONDS),
            new Duration(28, NANOSECONDS),
            Optional.empty(),
            MERGEABLE_INFO);

    @Test
    public void testJson()
    {
        JsonCodec<OperatorStats> codec = JsonCodec.jsonCodec(OperatorStats.class);

        String json = codec.toJson(EXPECTED);
        OperatorStats actual = codec.fromJson(json);

        assertExpectedOperatorStats(actual);
    }

    public static void assertExpectedOperatorStats(OperatorStats actual)
    {
        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 1);
        assertEquals(actual.getAddInputCalls(), 2);
        assertEquals(actual.getAddInputWall(), new Duration(3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getPhysicalInputDataSize(), new DataSize(51, BYTE));
        assertEquals(actual.getPhysicalInputPositions(), 511);
        assertEquals(actual.getInternalNetworkInputDataSize(), new DataSize(52, BYTE));
        assertEquals(actual.getInternalNetworkInputPositions(), 522);
        assertEquals(actual.getRawInputDataSize(), new DataSize(5, BYTE));
        assertEquals(actual.getInputDataSize(), new DataSize(6, BYTE));
        assertEquals(actual.getInputPositions(), 7);
        assertEquals(actual.getSumSquaredInputPositions(), 8.0);

        assertEquals(actual.getGetOutputCalls(), 9);
        assertEquals(actual.getGetOutputWall(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(11, NANOSECONDS));
        assertEquals(actual.getOutputDataSize(), new DataSize(12, BYTE));
        assertEquals(actual.getOutputPositions(), 13);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(14, BYTE));

        assertEquals(actual.getBlockedWall(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 16);
        assertEquals(actual.getFinishWall(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(18, NANOSECONDS));

        assertEquals(actual.getUserMemoryReservation(), new DataSize(19, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(20, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(21, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakSystemMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakRevocableMemoryReservation(), new DataSize(24, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(25, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(26, BYTE));
        assertEquals(actual.getSpillReadTime(), new Duration(27, NANOSECONDS));
        assertEquals(actual.getSpillWriteTime(), new Duration(28, NANOSECONDS));
        assertEquals(actual.getInfo().getClass(), SplitOperatorInfo.class);
        assertEquals(((SplitOperatorInfo) actual.getInfo()).getSplitInfo(), NON_MERGEABLE_INFO.getSplitInfo());
    }

    @Test
    public void testAdd()
    {
        OperatorStats actual = EXPECTED.add(EXPECTED, EXPECTED);

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getPhysicalInputDataSize(), new DataSize(3 * 51, BYTE));
        assertEquals(actual.getPhysicalInputPositions(), 3 * 511);
        assertEquals(actual.getInternalNetworkInputDataSize(), new DataSize(3 * 52, BYTE));
        assertEquals(actual.getInternalNetworkInputPositions(), 3 * 522);
        assertEquals(actual.getRawInputDataSize(), new DataSize(3 * 5, BYTE));
        assertEquals(actual.getInputDataSize(), new DataSize(3 * 6, BYTE));
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getOutputDataSize(), new DataSize(3 * 12, BYTE));
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(3 * 14, BYTE));

        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getUserMemoryReservation(), new DataSize(3 * 19, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(3 * 20, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(3 * 21, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakSystemMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakRevocableMemoryReservation(), new DataSize(24, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(25, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(3 * 26, BYTE));
        assertEquals(actual.getSpillReadTime(), new Duration(3 * 27, NANOSECONDS));
        assertEquals(actual.getSpillWriteTime(), new Duration(3 * 28, NANOSECONDS));
        assertNull(actual.getInfo());
    }

    @Test
    public void testAddMergeable()
    {
        OperatorStats actual = MERGEABLE.add(MERGEABLE, MERGEABLE);

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getPhysicalInputDataSize(), new DataSize(3 * 51, BYTE));
        assertEquals(actual.getPhysicalInputPositions(), 3 * 511);
        assertEquals(actual.getInternalNetworkInputDataSize(), new DataSize(3 * 52, BYTE));
        assertEquals(actual.getInternalNetworkInputPositions(), 3 * 522);
        assertEquals(actual.getRawInputDataSize(), new DataSize(3 * 5, BYTE));
        assertEquals(actual.getInputDataSize(), new DataSize(3 * 6, BYTE));
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getOutputDataSize(), new DataSize(3 * 12, BYTE));
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getPhysicalWrittenDataSize(), new DataSize(3 * 14, BYTE));

        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getUserMemoryReservation(), new DataSize(3 * 19, BYTE));
        assertEquals(actual.getRevocableMemoryReservation(), new DataSize(3 * 20, BYTE));
        assertEquals(actual.getSystemMemoryReservation(), new DataSize(3 * 21, BYTE));
        assertEquals(actual.getPeakUserMemoryReservation(), new DataSize(22, BYTE));
        assertEquals(actual.getPeakSystemMemoryReservation(), new DataSize(23, BYTE));
        assertEquals(actual.getPeakRevocableMemoryReservation(), new DataSize(24, BYTE));
        assertEquals(actual.getPeakTotalMemoryReservation(), new DataSize(25, BYTE));
        assertEquals(actual.getSpilledDataSize(), new DataSize(3 * 26, BYTE));
        assertEquals(actual.getSpillReadTime(), new Duration(3 * 27, NANOSECONDS));
        assertEquals(actual.getSpillWriteTime(), new Duration(3 * 28, NANOSECONDS));
        assertEquals(actual.getInfo().getClass(), PartitionedOutputInfo.class);
        assertEquals(((PartitionedOutputInfo) actual.getInfo()).getPagesAdded(), 3 * MERGEABLE_INFO.getPagesAdded());
    }
}
