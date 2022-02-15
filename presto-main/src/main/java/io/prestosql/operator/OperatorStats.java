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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.util.Mergeable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Immutable
public class OperatorStats
{
    private final int stageId;
    private final int pipelineId;
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;

    private final long totalDrivers;

    private final long addInputCalls;
    private final Duration addInputWall;
    private final Duration addInputCpu;
    private final DataSize physicalInputDataSize;
    private final long physicalInputPositions;
    private final DataSize internalNetworkInputDataSize;
    private final long internalNetworkInputPositions;
    private final DataSize rawInputDataSize;
    private final DataSize inputDataSize;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    private final long getOutputCalls;
    private final Duration getOutputWall;
    private final Duration getOutputCpu;
    private final DataSize outputDataSize;
    private final long outputPositions;

    private final DataSize physicalWrittenDataSize;

    private final Duration blockedWall;

    private final long finishCalls;
    private final Duration finishWall;
    private final Duration finishCpu;

    private final DataSize userMemoryReservation;
    private final DataSize revocableMemoryReservation;
    private final DataSize systemMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakSystemMemoryReservation;
    private final DataSize peakRevocableMemoryReservation;
    private final DataSize peakTotalMemoryReservation;

    private final DataSize spilledDataSize;
    private final Duration spillReadTime;
    private final Duration spillWriteTime;

    private final Optional<BlockedReason> blockedReason;

    private final OperatorInfo info;

    @JsonCreator
    public OperatorStats(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("pipelineId") int pipelineId,
            @JsonProperty("operatorId") int operatorId,
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("operatorType") String operatorType,

            @JsonProperty("totalDrivers") long totalDrivers,

            @JsonProperty("addInputCalls") long addInputCalls,
            @JsonProperty("addInputWall") Duration addInputWall,
            @JsonProperty("addInputCpu") Duration addInputCpu,
            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("physicalInputPositions") long physicalInputPositions,
            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("internalNetworkInputPositions") long internalNetworkInputPositions,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("inputDataSize") DataSize inputDataSize,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions,

            @JsonProperty("getOutputCalls") long getOutputCalls,
            @JsonProperty("getOutputWall") Duration getOutputWall,
            @JsonProperty("getOutputCpu") Duration getOutputCpu,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,

            @JsonProperty("blockedWall") Duration blockedWall,

            @JsonProperty("finishCalls") long finishCalls,
            @JsonProperty("finishWall") Duration finishWall,
            @JsonProperty("finishCpu") Duration finishCpu,

            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("systemMemoryReservation") DataSize systemMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakSystemMemoryReservation") DataSize peakSystemMemoryReservation,
            @JsonProperty("peakRevocableMemoryReservation") DataSize peakRevocableMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,

            @JsonProperty("spilledDataSize") DataSize spilledDataSize,
            @JsonProperty("spillReadTime") Duration spillReadTime,
            @JsonProperty("spillWriteTime") Duration spillWriteTime,

            @JsonProperty("blockedReason") Optional<BlockedReason> blockedReason,

            @JsonProperty("info") OperatorInfo info)
    {
        this.stageId = stageId;
        this.pipelineId = pipelineId;

        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        this.physicalInputDataSize = requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        this.physicalInputPositions = physicalInputPositions;
        this.internalNetworkInputDataSize = requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        this.internalNetworkInputPositions = internalNetworkInputPositions;
        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        this.inputDataSize = requireNonNull(inputDataSize, "inputDataSize is null");
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "writtenDataSize is null");

        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");

        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");
        this.systemMemoryReservation = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null");

        this.peakUserMemoryReservation = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        this.peakSystemMemoryReservation = requireNonNull(peakSystemMemoryReservation, "peakSystemMemoryReservation is null");
        this.peakRevocableMemoryReservation = requireNonNull(peakRevocableMemoryReservation, "peakRevocableMemoryReservation is null");
        this.peakTotalMemoryReservation = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null");

        this.spilledDataSize = requireNonNull(spilledDataSize, "spilledDataSize is null");
        this.spillReadTime = requireNonNull(spillReadTime, "spillReadTime is null");
        this.spillWriteTime = requireNonNull(spillWriteTime, "spillWriteTime is null");

        this.blockedReason = blockedReason;

        this.info = info;
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getPipelineId()
    {
        return pipelineId;
    }

    @JsonProperty
    public int getOperatorId()
    {
        return operatorId;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public String getOperatorType()
    {
        return operatorType;
    }

    @JsonProperty
    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public long getAddInputCalls()
    {
        return addInputCalls;
    }

    @JsonProperty
    public Duration getAddInputWall()
    {
        return addInputWall;
    }

    @JsonProperty
    public Duration getAddInputCpu()
    {
        return addInputCpu;
    }

    @JsonProperty
    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    @JsonProperty
    public long getPhysicalInputPositions()
    {
        return physicalInputPositions;
    }

    @JsonProperty
    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
    }

    @JsonProperty
    public long getInternalNetworkInputPositions()
    {
        return internalNetworkInputPositions;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public DataSize getInputDataSize()
    {
        return inputDataSize;
    }

    @JsonProperty
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    @JsonProperty
    public long getGetOutputCalls()
    {
        return getOutputCalls;
    }

    @JsonProperty
    public Duration getGetOutputWall()
    {
        return getOutputWall;
    }

    @JsonProperty
    public Duration getGetOutputCpu()
    {
        return getOutputCpu;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public Duration getBlockedWall()
    {
        return blockedWall;
    }

    @JsonProperty
    public long getFinishCalls()
    {
        return finishCalls;
    }

    @JsonProperty
    public Duration getFinishWall()
    {
        return finishWall;
    }

    @JsonProperty
    public Duration getFinishCpu()
    {
        return finishCpu;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    public DataSize getSystemMemoryReservation()
    {
        return systemMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakRevocableMemoryReservation()
    {
        return peakRevocableMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakSystemMemoryReservation()
    {
        return peakSystemMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public DataSize getSpilledDataSize()
    {
        return spilledDataSize;
    }

    @JsonProperty
    public Duration getSpillReadTime()
    {
        return spillReadTime;
    }

    @JsonProperty
    public Duration getSpillWriteTime()
    {
        return spillWriteTime;
    }

    @JsonProperty
    public Optional<BlockedReason> getBlockedReason()
    {
        return blockedReason;
    }

    @Nullable
    @JsonProperty
    public OperatorInfo getInfo()
    {
        return info;
    }

    public OperatorStats add(OperatorStats... operators)
    {
        return add(ImmutableList.copyOf(operators));
    }

    public OperatorStats add(Iterable<OperatorStats> operators)
    {
        long totalDriverNumber = this.totalDrivers;

        long addInputCallNumber = this.addInputCalls;
        long addInputWallTime = this.addInputWall.roundTo(NANOSECONDS);
        long addInputCpuTime = this.addInputCpu.roundTo(NANOSECONDS);
        long currentPhysicalInputDataSize = this.physicalInputDataSize.toBytes();
        long currentPhysicalInputPositions = this.physicalInputPositions;
        long currentInternalNetworkInputDataSize = this.internalNetworkInputDataSize.toBytes();
        long currentInternalNetworkInputPositions = this.internalNetworkInputPositions;
        long currentRawInputDataSize = this.rawInputDataSize.toBytes();
        long currentInputDataSize = this.inputDataSize.toBytes();
        long currentInputPositions = this.inputPositions;
        double squaredInputPositions = this.sumSquaredInputPositions;

        long outputCallNumber = this.getOutputCalls;
        long outputWallTime = this.getOutputWall.roundTo(NANOSECONDS);
        long outputCpuTime = this.getOutputCpu.roundTo(NANOSECONDS);
        long currentOutputDataSize = this.outputDataSize.toBytes();
        long currentOutputPositions = this.outputPositions;

        long currentPhysicalWrittenDataSize = this.physicalWrittenDataSize.toBytes();

        long blockedWallTime = this.blockedWall.roundTo(NANOSECONDS);

        long finishCallNumber = this.finishCalls;
        long finishWallTime = this.finishWall.roundTo(NANOSECONDS);
        long finishCpuTime = this.finishCpu.roundTo(NANOSECONDS);

        long memoryReservation = this.userMemoryReservation.toBytes();
        long revocableMemoryReservationBytes = this.revocableMemoryReservation.toBytes();
        long systemMemoryReservationBytes = this.systemMemoryReservation.toBytes();
        long peakUserMemory = this.peakUserMemoryReservation.toBytes();
        long peakSystemMemory = this.peakSystemMemoryReservation.toBytes();
        long peakRevocableMemory = this.peakRevocableMemoryReservation.toBytes();
        long peakTotalMemory = this.peakTotalMemoryReservation.toBytes();

        long currentSpilledDataSize = this.spilledDataSize.toBytes();
        long spillRead = this.spillReadTime.roundTo(NANOSECONDS);
        long spillWrite = this.spillWriteTime.roundTo(NANOSECONDS);

        Optional<BlockedReason> blockedReasonOptional = this.blockedReason;

        Mergeable<OperatorInfo> base = getMergeableInfoOrNull(info);
        for (OperatorStats operator : operators) {
            checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());

            totalDriverNumber += operator.totalDrivers;

            addInputCallNumber += operator.getAddInputCalls();
            addInputWallTime += operator.getAddInputWall().roundTo(NANOSECONDS);
            addInputCpuTime += operator.getAddInputCpu().roundTo(NANOSECONDS);
            currentPhysicalInputDataSize += operator.getPhysicalInputDataSize().toBytes();
            currentPhysicalInputPositions += operator.getPhysicalInputPositions();
            currentInternalNetworkInputDataSize += operator.getInternalNetworkInputDataSize().toBytes();
            currentInternalNetworkInputPositions += operator.getInternalNetworkInputPositions();
            currentRawInputDataSize += operator.getRawInputDataSize().toBytes();
            currentInputDataSize += operator.getInputDataSize().toBytes();
            currentInputPositions += operator.getInputPositions();
            squaredInputPositions += operator.getSumSquaredInputPositions();

            outputCallNumber += operator.getGetOutputCalls();
            outputWallTime += operator.getGetOutputWall().roundTo(NANOSECONDS);
            outputCpuTime += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            currentOutputDataSize += operator.getOutputDataSize().toBytes();
            currentOutputPositions += operator.getOutputPositions();

            currentPhysicalWrittenDataSize += operator.getPhysicalWrittenDataSize().toBytes();

            finishCallNumber += operator.getFinishCalls();
            finishWallTime += operator.getFinishWall().roundTo(NANOSECONDS);
            finishCpuTime += operator.getFinishCpu().roundTo(NANOSECONDS);

            blockedWallTime += operator.getBlockedWall().roundTo(NANOSECONDS);

            memoryReservation += operator.getUserMemoryReservation().toBytes();
            revocableMemoryReservationBytes += operator.getRevocableMemoryReservation().toBytes();
            systemMemoryReservationBytes += operator.getSystemMemoryReservation().toBytes();

            peakUserMemory = max(peakUserMemory, operator.getPeakUserMemoryReservation().toBytes());
            peakSystemMemory = max(peakSystemMemory, operator.getPeakSystemMemoryReservation().toBytes());
            peakRevocableMemory = max(peakRevocableMemory, operator.getPeakRevocableMemoryReservation().toBytes());
            peakTotalMemory = max(peakTotalMemory, operator.getPeakTotalMemoryReservation().toBytes());

            currentSpilledDataSize += operator.getSpilledDataSize().toBytes();
            spillRead += operator.getSpillReadTime().roundTo(NANOSECONDS);
            spillWrite += operator.getSpillWriteTime().roundTo(NANOSECONDS);

            if (operator.getBlockedReason().isPresent()) {
                blockedReasonOptional = operator.getBlockedReason();
            }

            OperatorInfo operatorInfo = operator.getInfo();
            if (base != null && operatorInfo != null && base.getClass() == operatorInfo.getClass()) {
                base = mergeInfo(base, operatorInfo);
            }
        }

        return new OperatorStats(
                stageId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,

                totalDriverNumber,

                addInputCallNumber,
                new Duration(addInputWallTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(currentPhysicalInputDataSize),
                currentPhysicalInputPositions,
                succinctBytes(currentInternalNetworkInputDataSize),
                currentInternalNetworkInputPositions,
                succinctBytes(currentRawInputDataSize),
                succinctBytes(currentInputDataSize),
                currentInputPositions,
                squaredInputPositions,

                outputCallNumber,
                new Duration(outputWallTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(outputCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(currentOutputDataSize),
                currentOutputPositions,

                succinctBytes(currentPhysicalWrittenDataSize),

                new Duration(blockedWallTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishCallNumber,
                new Duration(finishWallTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                succinctBytes(memoryReservation),
                succinctBytes(revocableMemoryReservationBytes),
                succinctBytes(systemMemoryReservationBytes),
                succinctBytes(peakUserMemory),
                succinctBytes(peakSystemMemory),
                succinctBytes(peakRevocableMemory),
                succinctBytes(peakTotalMemory),

                succinctBytes(currentSpilledDataSize),
                new Duration(spillRead, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(spillWrite, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                blockedReasonOptional,

                (OperatorInfo) base);
    }

    @SuppressWarnings("unchecked")
    private static Mergeable<OperatorInfo> getMergeableInfoOrNull(OperatorInfo info)
    {
        Mergeable<OperatorInfo> base = null;
        if (info instanceof Mergeable) {
            base = (Mergeable<OperatorInfo>) info;
        }
        return base;
    }

    @SuppressWarnings("unchecked")
    private static <T> Mergeable<T> mergeInfo(Mergeable<T> base, T other)
    {
        return (Mergeable<T>) base.mergeWith(other);
    }

    public OperatorStats summarize()
    {
        return new OperatorStats(
                stageId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,
                totalDrivers,
                addInputCalls,
                addInputWall,
                addInputCpu,
                physicalInputDataSize,
                physicalInputPositions,
                internalNetworkInputDataSize,
                internalNetworkInputPositions,
                rawInputDataSize,
                inputDataSize,
                inputPositions,
                sumSquaredInputPositions,
                getOutputCalls,
                getOutputWall,
                getOutputCpu,
                outputDataSize,
                outputPositions,
                physicalWrittenDataSize,
                blockedWall,
                finishCalls,
                finishWall,
                finishCpu,
                userMemoryReservation,
                revocableMemoryReservation,
                systemMemoryReservation,
                peakUserMemoryReservation,
                peakSystemMemoryReservation,
                peakRevocableMemoryReservation,
                peakTotalMemoryReservation,
                spilledDataSize,
                spillReadTime,
                spillWriteTime,
                blockedReason,
                (info != null && info.isFinal()) ? info : null);
    }
}
