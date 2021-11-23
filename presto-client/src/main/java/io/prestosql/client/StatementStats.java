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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.OptionalDouble;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@Immutable
public class StatementStats
{
    private final String state;
    private final boolean queued;
    private final boolean scheduled;
    private final int nodes;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long queuedTimeMillis;
    private final long elapsedTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final long peakMemoryBytes;
    private final long spilledBytes;
    private long elapsedSpillReadTimeMillis;
    private long elapsedSpillWriteTimeMillis;
    private final StageStats rootStage;

    @JsonCreator
    public StatementStats(
            @JsonProperty("state") String state,
            @JsonProperty("queued") boolean queued,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("nodes") int nodes,
            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("cpuTimeMillis") long cpuTimeMillis,
            @JsonProperty("wallTimeMillis") long wallTimeMillis,
            @JsonProperty("queuedTimeMillis") long queuedTimeMillis,
            @JsonProperty("elapsedTimeMillis") long elapsedTimeMillis,
            @JsonProperty("processedRows") long processedRows,
            @JsonProperty("processedBytes") long processedBytes,
            @JsonProperty("peakMemoryBytes") long peakMemoryBytes,
            @JsonProperty("spilledBytes") long spilledBytes,
            @JsonProperty("elapsedSpillReadTimeMillis") long elapsedSpillReadTimeMillis,
            @JsonProperty("elapsedSpillWriteTimeMillis") long elapsedSpillWriteTimeMillis,
            @JsonProperty("rootStage") StageStats rootStage)
    {
        this.state = requireNonNull(state, "state is null");
        this.queued = queued;
        this.scheduled = scheduled;
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.peakMemoryBytes = peakMemoryBytes;
        this.spilledBytes = spilledBytes;
        this.elapsedSpillReadTimeMillis = elapsedSpillReadTimeMillis;
        this.elapsedSpillWriteTimeMillis = elapsedSpillWriteTimeMillis;
        this.rootStage = rootStage;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isQueued()
    {
        return queued;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public int getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @JsonProperty
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @JsonProperty
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @JsonProperty
    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    @JsonProperty
    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    @JsonProperty
    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    @JsonProperty
    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    @JsonProperty
    public long getProcessedRows()
    {
        return processedRows;
    }

    @JsonProperty
    public long getProcessedBytes()
    {
        return processedBytes;
    }

    @JsonProperty
    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @Nullable
    @JsonProperty
    public StageStats getRootStage()
    {
        return rootStage;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        if (!scheduled || totalSplits == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(min(100, (completedSplits * 100.0) / totalSplits));
    }

    @JsonProperty
    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    @JsonProperty
    public long getElapsedSpillReadTimeMillis()
    {
        return elapsedSpillReadTimeMillis;
    }

    @JsonProperty
    public long getElapsedSpillWriteTimeMillis()
    {
        return elapsedSpillWriteTimeMillis;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("state", state)
                .add("queued", queued)
                .add("scheduled", scheduled)
                .add("nodes", nodes)
                .add("totalSplits", totalSplits)
                .add("queuedSplits", queuedSplits)
                .add("runningSplits", runningSplits)
                .add("completedSplits", completedSplits)
                .add("cpuTimeMillis", cpuTimeMillis)
                .add("wallTimeMillis", wallTimeMillis)
                .add("queuedTimeMillis", queuedTimeMillis)
                .add("elapsedTimeMillis", elapsedTimeMillis)
                .add("processedRows", processedRows)
                .add("processedBytes", processedBytes)
                .add("peakMemoryBytes", peakMemoryBytes)
                .add("spilledBytes", spilledBytes)
                .add("elapsedSpillReadTime", elapsedSpillReadTimeMillis)
                .add("elapsedSpillWriteTime", elapsedSpillWriteTimeMillis)
                .add("rootStage", rootStage)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String state;
        private boolean queued;
        private boolean scheduled;
        private int nodes;
        private int totalSplits;
        private int queuedSplits;
        private int runningSplits;
        private int completedSplits;
        private long cpuTimeMillis;
        private long wallTimeMillis;
        private long queuedTimeMillis;
        private long elapsedTimeMillis;
        private long processedRows;
        private long processedBytes;
        private long peakMemoryBytes;
        private long spilledBytes;
        private StageStats rootStage;
        private long spillReadTimeMillis;
        private long spillWriteTimeMillis;

        private Builder() {}

        public Builder setState(String state)
        {
            this.state = requireNonNull(state, "state is null");
            return this;
        }

        public Builder setNodes(int nodes)
        {
            this.nodes = nodes;
            return this;
        }

        public Builder setQueued(boolean queued)
        {
            this.queued = queued;
            return this;
        }

        public Builder setScheduled(boolean scheduled)
        {
            this.scheduled = scheduled;
            return this;
        }

        public Builder setTotalSplits(int totalSplits)
        {
            this.totalSplits = totalSplits;
            return this;
        }

        public Builder setQueuedSplits(int queuedSplits)
        {
            this.queuedSplits = queuedSplits;
            return this;
        }

        public Builder setRunningSplits(int runningSplits)
        {
            this.runningSplits = runningSplits;
            return this;
        }

        public Builder setCompletedSplits(int completedSplits)
        {
            this.completedSplits = completedSplits;
            return this;
        }

        public Builder setCpuTimeMillis(long cpuTimeMillis)
        {
            this.cpuTimeMillis = cpuTimeMillis;
            return this;
        }

        public Builder setWallTimeMillis(long wallTimeMillis)
        {
            this.wallTimeMillis = wallTimeMillis;
            return this;
        }

        public Builder setQueuedTimeMillis(long queuedTimeMillis)
        {
            this.queuedTimeMillis = queuedTimeMillis;
            return this;
        }

        public Builder setElapsedTimeMillis(long elapsedTimeMillis)
        {
            this.elapsedTimeMillis = elapsedTimeMillis;
            return this;
        }

        public Builder setProcessedRows(long processedRows)
        {
            this.processedRows = processedRows;
            return this;
        }

        public Builder setProcessedBytes(long processedBytes)
        {
            this.processedBytes = processedBytes;
            return this;
        }

        public Builder setPeakMemoryBytes(long peakMemoryBytes)
        {
            this.peakMemoryBytes = peakMemoryBytes;
            return this;
        }

        public Builder setSpilledBytes(long spilledBytes)
        {
            this.spilledBytes = spilledBytes;
            return this;
        }

        public Builder setSpilledWriteTimeMillis(long spillTime)
        {
            this.spillWriteTimeMillis = spillTime;
            return this;
        }

        public Builder setSpilledReadTimeMillis(long spillTime)
        {
            this.spillReadTimeMillis = spillTime;
            return this;
        }

        public Builder setRootStage(StageStats rootStage)
        {
            this.rootStage = rootStage;
            return this;
        }

        public StatementStats build()
        {
            return new StatementStats(
                    state,
                    queued,
                    scheduled,
                    nodes,
                    totalSplits,
                    queuedSplits,
                    runningSplits,
                    completedSplits,
                    cpuTimeMillis,
                    wallTimeMillis,
                    queuedTimeMillis,
                    elapsedTimeMillis,
                    processedRows,
                    processedBytes,
                    peakMemoryBytes,
                    spilledBytes,
                    spillReadTimeMillis,
                    spillWriteTimeMillis,
                    rootStage);
        }
    }
}
