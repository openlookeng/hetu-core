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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.plugin.hive.HiveSplit.BucketConversion;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.InternalHiveSplit;
import io.prestosql.plugin.hive.S3SelectPushdown;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.hive.HiveColumnHandle.isPathColumnHandle;
import static io.prestosql.plugin.hive.HiveUtil.isSplittable;
import static io.prestosql.plugin.hive.util.CustomSplitConversionUtils.extractCustomSplitInfo;
import static java.util.Objects.requireNonNull;

public class InternalHiveSplitFactory
{
    private final FileSystem fileSystem;
    private final String partitionName;
    private final InputFormat<?, ?> inputFormat;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final Optional<Domain> pathDomain;
    private final Map<Integer, HiveTypeName> columnCoercions;
    private final Optional<BucketConversion> bucketConversion;
    private final boolean forceLocalScheduling;
    private final boolean s3SelectPushdownEnabled;

    public InternalHiveSplitFactory(
            FileSystem fileSystem,
            String partitionName,
            InputFormat<?, ?> inputFormat,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Map<Integer, HiveTypeName> columnCoercions,
            Optional<BucketConversion> bucketConversion,
            boolean forceLocalScheduling,
            boolean s3SelectPushdownEnabled)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        pathDomain = getPathDomain(requireNonNull(effectivePredicate, "effectivePredicate is null"));
        this.columnCoercions = requireNonNull(columnCoercions, "columnCoercions is null");
        this.bucketConversion = requireNonNull(bucketConversion, "bucketConversion is null");
        this.forceLocalScheduling = forceLocalScheduling;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(LocatedFileStatus status, boolean splittable, Optional<DeleteDeltaLocations> deleteDeltaLocations, Optional<Long> startRowOffsetOfFile)
    {
        return createInternalHiveSplit(status, OptionalInt.empty(), splittable, deleteDeltaLocations, startRowOffsetOfFile);
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(LocatedFileStatus status, int bucketNumber, Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        return createInternalHiveSplit(status, OptionalInt.of(bucketNumber), false, deleteDeltaLocations, Optional.empty());
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(LocatedFileStatus status, OptionalInt bucketNumber, boolean splittable, Optional<DeleteDeltaLocations> deleteDeltaLocations, Optional<Long> startRowOffsetOfFile)
    {
        boolean tmpSplittable = splittable && isSplittable(inputFormat, fileSystem, status.getPath());
        return createInternalHiveSplit(
                status.getPath(),
                status.getBlockLocations(),
                0,
                status.getLen(),
                status.getLen(),
                status.getModificationTime(),
                bucketNumber,
                tmpSplittable,
                deleteDeltaLocations,
                startRowOffsetOfFile,
                ImmutableMap.of());
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(FileSplit split)
            throws IOException
    {
        FileStatus file = fileSystem.getFileStatus(split.getPath());
        Map<String, String> customSplitInfo = extractCustomSplitInfo(split);
        return createInternalHiveSplit(
                split.getPath(),
                fileSystem.getFileBlockLocations(file, split.getStart(), split.getLength()),
                split.getStart(),
                split.getLength(),
                file.getLen(),
                file.getModificationTime(),
                OptionalInt.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                customSplitInfo);
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(
            Path path,
            BlockLocation[] blockLocations,
            long start,
            long length,
            long fileSize,
            long lastModifiedTime,
            OptionalInt bucketNumber,
            boolean splittable,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Map<String, String> customSplitInfo)
    {
        String pathString = path.toString();
        if (!pathMatchesPredicate(pathDomain, pathString)) {
            return Optional.empty();
        }

        boolean tmpForceLocalScheduling = this.forceLocalScheduling;

        // For empty files, some filesystem (e.g. LocalFileSystem) produce one empty block
        // while others (e.g. hdfs.DistributedFileSystem) produces no block.
        // Synthesize an empty block if one does not already exist.
        BlockLocation[] tmpBlockLocations = blockLocations;
        if (fileSize == 0 && tmpBlockLocations.length == 0) {
            tmpBlockLocations = new BlockLocation[] {new BlockLocation()};
            // Turn off force local scheduling because hosts list doesn't exist.
            tmpForceLocalScheduling = false;
        }

        ImmutableList.Builder<InternalHiveSplit.InternalHiveBlock> blockBuilder = ImmutableList.builder();
        for (BlockLocation blockLocation : tmpBlockLocations) {
            // clamp the block range
            long blockStart = Math.max(start, blockLocation.getOffset());
            long blockEnd = Math.min(start + length, blockLocation.getOffset() + blockLocation.getLength());
            if (blockStart > blockEnd) {
                // block is outside split range
                continue;
            }
            if (blockStart == blockEnd && !(blockStart == start && blockEnd == start + length)) {
                // skip zero-width block, except in the special circumstance: slice is empty, and the block covers the empty slice interval.
                continue;
            }
            blockBuilder.add(new InternalHiveSplit.InternalHiveBlock(blockStart, blockEnd, getHostAddresses(blockLocation)));
        }
        List<InternalHiveSplit.InternalHiveBlock> blocks = blockBuilder.build();
        checkBlocks(blocks, start, length);

        if (!splittable) {
            // not splittable, use the hosts from the first block if it exists
            blocks = ImmutableList.of(new InternalHiveSplit.InternalHiveBlock(start, start + length, blocks.get(0).getAddresses()));
        }

        return Optional.of(new InternalHiveSplit(
                partitionName,
                pathString,
                start,
                start + length,
                fileSize,
                lastModifiedTime,
                schema,
                partitionKeys,
                blocks,
                bucketNumber,
                splittable,
                tmpForceLocalScheduling && allBlocksHaveAddress(blocks),
                columnCoercions,
                bucketConversion,
                s3SelectPushdownEnabled && S3SelectPushdown.isCompressionCodecSupported(inputFormat, path),
                deleteDeltaLocations,
                startRowOffsetOfFile,
                customSplitInfo));
    }

    private static void checkBlocks(List<InternalHiveSplit.InternalHiveBlock> blocks, long start, long length)
    {
        checkArgument(length >= 0);
        checkArgument(!blocks.isEmpty());
        checkArgument(start == blocks.get(0).getStart());
        checkArgument(start + length == blocks.get(blocks.size() - 1).getEnd());
        for (int i = 1; i < blocks.size(); i++) {
            checkArgument(blocks.get(i - 1).getEnd() == blocks.get(i).getStart());
        }
    }

    private static boolean allBlocksHaveAddress(Collection<InternalHiveSplit.InternalHiveBlock> blocks)
    {
        return blocks.stream()
                .map(InternalHiveSplit.InternalHiveBlock::getAddresses)
                .noneMatch(List::isEmpty);
    }

    private static List<HostAddress> getHostAddresses(BlockLocation blockLocation)
    {
        // Hadoop FileSystem returns "localhost" as a default
        return Arrays.stream(getBlockHosts(blockLocation))
                .map(HostAddress::fromString)
                .filter(address -> !address.getHostText().equals("localhost"))
                .collect(toImmutableList());
    }

    private static String[] getBlockHosts(BlockLocation blockLocation)
    {
        try {
            return blockLocation.getHosts();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Optional<Domain> getPathDomain(TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        return effectivePredicate.getDomains()
                .flatMap(domains -> domains.entrySet().stream()
                        .filter(entry -> isPathColumnHandle(entry.getKey()))
                        .map(Map.Entry::getValue)
                        .findFirst());
    }

    private static boolean pathMatchesPredicate(Optional<Domain> pathDomain, String path)
    {
        return pathDomain
                .map(domain -> domain.includesNullableValue(utf8Slice(path)))
                .orElse(true);
    }
}
