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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HiveUtil;
import io.prestosql.plugin.hive.WriteIdInfo;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;

@NotThreadSafe
public class OrcDeletedRows
{
    private final String sourceFileName;
    private final DeleteDeltaLocations deleteDeltaLocations;
    private final OrcDeleteDeltaPageSourceFactory pageSourceFactory;
    private final String sessionUser;
    private final Configuration configuration;
    private final HdfsEnvironment hdfsEnvironment;
    Optional<Long> startRowOffsetOfFile;

    private final Pattern originalFilePattern = Pattern.compile("[0-9]+_[0-9]+");
    private final Pattern originalCopyFilePattern = Pattern.compile("[0-9]+_[0-9]+" + "_copy_" + "[0-9]+");
    private final String bucketPrefix = "bucket_";

    private final List<ConnectorPageSource> pageSources = new ArrayList<>();
    private Iterator<Page> sortedRowsIterator;
    private Page currentPage;
    private int currentPageOffset;
    private OrcAcidRowId deletedRowId = new OrcAcidRowId(0, 0, 0);

    public OrcDeletedRows(
            String sourceFileName,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            OrcDeleteDeltaPageSourceFactory pageSourceFactory,
            String sessionUser,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment,
            Optional<Long> startRowOffsetOfFile)
    {
        this.sourceFileName = sourceFileName;
        this.pageSourceFactory = pageSourceFactory;
        this.sessionUser = sessionUser;
        this.configuration = configuration;
        this.hdfsEnvironment = hdfsEnvironment;
        if (deleteDeltaLocations.isPresent()) {
            this.deleteDeltaLocations = deleteDeltaLocations.get();
        }
        else {
            this.deleteDeltaLocations = null;
        }
        this.startRowOffsetOfFile = startRowOffsetOfFile;
    }

    public MaskDeletedRowsFunction getMaskDeletedRowsFunction(Page sourcePage, Optional<Long> pageRowOffset)
    {
        return new MaskDeletedRowsFunction(sourcePage, pageRowOffset);
    }

    @NotThreadSafe
    public class MaskDeletedRowsFunction
    {
        @Nullable
        private Page sourcePage;
        private int positionCount;
        @Nullable
        private int[] validPositions;
        private Optional<Long> pageRowOffset;

        public MaskDeletedRowsFunction(Page sourcePage, Optional<Long> pageRowOffset)
        {
            this.sourcePage = sourcePage;
            this.pageRowOffset = pageRowOffset;
        }

        public int getPositionCount()
        {
            if (sourcePage != null) {
                loadValidPositions();
                verify(sourcePage == null);
            }

            return positionCount;
        }

        public Block apply(Block block)
        {
            if (sourcePage != null) {
                loadValidPositions();
                verify(sourcePage == null);
            }

            if (positionCount == block.getPositionCount() || block.getPositionCount() == 0 || validPositions == null) {
                return block;
            }
            return new DictionaryBlock(positionCount, block, validPositions);
        }

        private void loadValidPositions()
        {
            if (deleteDeltaLocations == null) {
                this.positionCount = sourcePage.getPositionCount();
                this.sourcePage = null;
                return;
            }

            int[] validPositions = new int[sourcePage.getPositionCount()];
            OrcAcidRowId sourcePageRowId = new OrcAcidRowId(0, 0, 0);
            int validPositionsIndex = 0;
            for (int pagePosition = 0; pagePosition < sourcePage.getPositionCount(); pagePosition++) {
                if (startRowOffsetOfFile.isPresent() && pageRowOffset.isPresent()) {
                    long currRowId = startRowOffsetOfFile.get() + pageRowOffset.get() + pagePosition;
                    sourcePageRowId.set(sourcePageRowId.getOriginalTransaction(), sourcePageRowId.getBucket(), currRowId);
                }
                else {
                    sourcePageRowId.set(sourcePage, pagePosition);
                }
                boolean deleted = isDeleted(sourcePageRowId);
                if (!deleted) {
                    validPositions[validPositionsIndex] = pagePosition;
                    validPositionsIndex++;
                }
            }
            this.positionCount = validPositionsIndex;
            this.validPositions = validPositions;
            this.sourcePage = null;
        }
    }

    private boolean isDeleted(OrcAcidRowId sourcePageRowId)
    {
        if (sortedRowsIterator == null) {
            for (WriteIdInfo deleteDeltaInfo : deleteDeltaLocations.getDeleteDeltas()) {
                Path path = createPath(deleteDeltaLocations.getPartitionLocation(), deleteDeltaInfo, sourceFileName);
                try {
                    FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
                    FileStatus fileStatus = hdfsEnvironment.doAs(sessionUser, () -> fileSystem.getFileStatus(path));

                    pageSources.add(pageSourceFactory.createPageSource(fileStatus.getPath(), fileStatus.getLen()));
                }
                catch (FileNotFoundException ignored) {
                    // source file does not have a delta delete file in this location
                    continue;
                }
                catch (PrestoException e) {
                    throw e;
                }
                catch (OrcCorruptionException e) {
                    throw new PrestoException(HiveErrorCode.HIVE_BAD_DATA, format("Failed to read ORC file: %s", path), e);
                }
                catch (RuntimeException | IOException e) {
                    throw new PrestoException(HiveErrorCode.HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", path), e);
                }
            }
            List<Type> columnTypes = ImmutableList.of(BigintType.BIGINT, IntegerType.INTEGER, BigintType.BIGINT);
            //Last index for rowIdHandle
            List<Integer> sortFields = ImmutableList.of(0, 1, 2);
            List<SortOrder> sortOrders = ImmutableList.of(SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST);
            sortedRowsIterator = HiveUtil.getMergeSortedPages(pageSources, columnTypes, sortFields,
                    sortOrders);
        }
        do {
            if (currentPage == null || currentPageOffset >= currentPage.getPositionCount()) {
                currentPage = null;
                currentPageOffset = 0;
                if (sortedRowsIterator.hasNext()) {
                    currentPage = sortedRowsIterator.next();
                }
                else {
                    //No more entries in deleted_delta
                    return false;
                }
            }
            do {
                deletedRowId.set(currentPage, currentPageOffset);
                if (deletedRowId.compareTo(sourcePageRowId) == 0) {
                    //source row is deleted.
                    return true;
                }
                else if (deletedRowId.compareTo(sourcePageRowId) > 0) {
                    //source row entry not found, but next deleted entry is greater than current source row.
                    //So current source row is not deleted.
                    return false;
                }
                currentPageOffset++;
            }
            while (currentPageOffset < currentPage.getPositionCount());
        }
        while (sortedRowsIterator.hasNext());
        //No more entries;
        return false;
    }

    private int getBucketNumber(String fileName)
    {
        if (fileName.startsWith(bucketPrefix)) {
            return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1));
        }
        else if (originalFilePattern.matcher(fileName).matches() || originalCopyFilePattern.matcher(fileName).matches()) {
            return Integer.parseInt(fileName.substring(0, fileName.indexOf('_')));
        }
        return -1;
    }

    private Path createPath(String partitionLocation, WriteIdInfo deleteDeltaInfo, String fileName)
    {
        //Honor statement=-1 as well. Because Minor compacted delta directories will not have statementId
        Path directory;
        if (deleteDeltaInfo.getStatementId() == -1) {
            directory = new Path(partitionLocation, deleteDeltaSubdir(
                    deleteDeltaInfo.getMinWriteId(),
                    deleteDeltaInfo.getMaxWriteId()));
        }
        else {
            directory = new Path(partitionLocation, deleteDeltaSubdir(
                    deleteDeltaInfo.getMinWriteId(),
                    deleteDeltaInfo.getMaxWriteId(),
                    deleteDeltaInfo.getStatementId()));
        }
        String bucketFileName = getBucketFileName(fileName);
        return new Path(directory, bucketFileName);
    }

    private String getBucketFileName(String fileName)
    {
        String bucketDigit = "%05d";
        int bucketNumber = getBucketNumber(fileName);
        return bucketPrefix + String.format(bucketDigit, bucketNumber);
    }

    void close()
    {
        pageSources.forEach(pageSource ->
        {
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
