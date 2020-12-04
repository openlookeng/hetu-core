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
package io.prestosql.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveWriter
{
    private final HiveFileWriter fileWriter;
    private final Optional<String> partitionName;
    private final PartitionUpdate.UpdateMode updateMode;
    private final String fileName;
    private final String writePath;
    private final String targetPath;
    private final Consumer<HiveWriter> onCommit;
    private final HiveWriterStats hiveWriterStats;

    private List<String> extraPartitionFiles;
    private List<String> miscData;
    private long rowCount;
    private long inputSizeInBytes;

    public HiveWriter(
            HiveFileWriter fileWriter,
            Optional<String> partitionName,
            PartitionUpdate.UpdateMode updateMode,
            String fileName,
            String writePath,
            String targetPath,
            Consumer<HiveWriter> onCommit,
            HiveWriterStats hiveWriterStats,
            List<String> extraPartitionFiles)
    {
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.updateMode = requireNonNull(updateMode, "updateMode is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.onCommit = requireNonNull(onCommit, "onCommit is null");
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
        this.extraPartitionFiles = (extraPartitionFiles != null) ? ImmutableList.copyOf(extraPartitionFiles) : Collections.emptyList();
        this.miscData = Collections.emptyList();
    }

    public long getWrittenBytes()
    {
        return fileWriter.getWrittenBytes();
    }

    public long getSystemMemoryUsage()
    {
        return fileWriter.getSystemMemoryUsage();
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public void append(Page dataPage)
    {
        // getRegionSizeInBytes for each row can be expensive; use getRetainedSizeInBytes for estimation
        hiveWriterStats.addInputPageSizesInBytes(dataPage.getRetainedSizeInBytes());
        fileWriter.appendRows(dataPage);
        rowCount += dataPage.getPositionCount();
        inputSizeInBytes += dataPage.getSizeInBytes();
    }

    public void commit()
    {
        fileWriter.commit();

        /* Fetch again to confirm if some new files added as part of the operations */
        Set<String> set = new HashSet<>(extraPartitionFiles);
        set.addAll(fileWriter.getExtraPartitionFiles());

        extraPartitionFiles = ImmutableList.copyOf(set);

        miscData = fileWriter.getMiscData();

        onCommit.accept(this);
    }

    long getValidationCpuNanos()
    {
        return fileWriter.getValidationCpuNanos();
    }

    public Optional<Runnable> getVerificationTask()
    {
        return fileWriter.getVerificationTask();
    }

    public void rollback()
    {
        fileWriter.rollback();
    }

    public PartitionUpdate getPartitionUpdate()
    {
        return new PartitionUpdate(
                partitionName.orElse(""),
                updateMode,
                writePath,
                targetPath,
                ImmutableList.<String>builder().addAll(extraPartitionFiles).add(fileName).build(),
                rowCount,
                inputSizeInBytes,
                fileWriter.getWrittenBytes(),
                miscData);
    }

    @VisibleForTesting
    public HiveFileWriter getFileWriter()
    {
        return fileWriter;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileWriter", fileWriter)
                .add("filePath", writePath + "/" + fileName)
                .toString();
    }
}
