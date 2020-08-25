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
package io.hetu.core.plugin.carbondata;

import io.prestosql.spi.PrestoException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.CompactionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class CarbondataHetuCompactorUtil
{
    private static final Logger LOG =
            LogServiceFactory.getLogService(CarbondataHetuCompactorUtil.class.getName());

    private CarbondataHetuCompactorUtil()
    {
    }

    public static List<List<LoadMetadataDetails>> identifyAndGroupSegmentsToBeMerged(
            CarbonLoadModel carbonLoadModel, Configuration configuration, CompactionType compaction, long majorCompactionSegSize, long minorCompactionSegCount)
    {
        requireNonNull(carbonLoadModel, "carbonLoadModel is null");
        requireNonNull(configuration, "Configuration is null");
        requireNonNull(compaction, "Compaction type is null");

        List<LoadMetadataDetails> carbonSegments = new ArrayList<>();
        for (LoadMetadataDetails load : carbonLoadModel.getLoadMetadataDetails()) {
            if (load.isCarbonFormat() && load.getSegmentStatus() == SegmentStatus.SUCCESS) {
                carbonSegments.add(load);
            }
        }

        List<List<LoadMetadataDetails>> groupedCompactionSegments = new ArrayList<>();
        List<String> skipSegList = new ArrayList<>();
        if (compaction == CompactionType.MAJOR) {
            do {
                List<LoadMetadataDetails> segments = identifySegmentsToBeMergedBasedOnSize(carbonSegments, carbonLoadModel, skipSegList, configuration, majorCompactionSegSize);
                populateSkipSegs(segments, skipSegList);
                if (segments.size() < 2) {
                    break;
                }
                groupedCompactionSegments.add(segments);
            }
            while (true);
        }
        else {
            do {
                List<LoadMetadataDetails> segments = identifySegmentsToBeMergedBasedOnSegCount(carbonSegments, skipSegList, minorCompactionSegCount);
                populateSkipSegs(segments, skipSegList);
                if (segments.size() < 2) {
                    break;
                }
                groupedCompactionSegments.add(segments);
            }
            while (true);
        }
        return groupedCompactionSegments;
    }

    private static void populateSkipSegs(List<LoadMetadataDetails> segList, List<String> skipSegList)
    {
        for (LoadMetadataDetails load : segList) {
            skipSegList.add(load.getLoadName());
        }
    }

    private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSize(List<LoadMetadataDetails> listOfSegmentsAfterPreserve,
            CarbonLoadModel carbonLoadModel, List<String> segmentsToBeSkipped, Configuration configuration, long majorVacuumSize)
    {
        List<LoadMetadataDetails> segmentsToBeMerged =
                new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();

        // total length
        long totalLength = 0;

        // check size of each segment , sum it up across partitions
        for (LoadMetadataDetails segment : listOfSegmentsAfterPreserve) {
            String segId = segment.getLoadName();
            if (segmentsToBeSkipped.contains(segId)) {
                continue;
            }
            // variable to store one  segment size across partition.
            long sizeOfOneSegmentAcrossPartition = 0;
            if (segment.getSegmentFile() != null) {
                // If LoadMetaDataDetail already has data size no need to calculate the data size from
                // index files. If not there then read the index file and calculate size.
                if (!StringUtils.isEmpty(segment.getDataSize())) {
                    sizeOfOneSegmentAcrossPartition = Long.parseLong(segment.getDataSize());
                }
                else {
                    try {
                        sizeOfOneSegmentAcrossPartition = CarbonUtil.getSizeOfSegment(carbonTable.getTablePath(),
                                new Segment(segId, segment.getSegmentFile()));
                    }
                    catch (IOException e) {
                        LOG.error("Size of segment is not available");
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while reading size of segment" + e);
                    }
                }
            }
            else {
                sizeOfOneSegmentAcrossPartition = getSizeOfSegment(carbonTable.getTablePath(), segId, configuration);
            }

            // if size of a segment is greater than the Major compaction size. then ignore it.
            if (sizeOfOneSegmentAcrossPartition > (majorVacuumSize * 1024 * 1024 * 1024)) {
                // if already 2 segments have been found for merging then stop scan here and merge.
                if (segmentsToBeMerged.size() > 1) {
                    break;
                }
                else { // if only one segment is found then remove the earlier one in list.
                    // reset the total length to 0.
                    segmentsToBeMerged = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                    totalLength = 0;
                    continue;
                }
            }

            totalLength += sizeOfOneSegmentAcrossPartition;

            // in case of major compaction the size doesn't matter. all the segments will be merged.
            if (totalLength < (majorVacuumSize * 1024 * 1024)) {
                segmentsToBeMerged.add(segment);
            }
            else { // if already 2 segments have been found for merging then stop scan here and merge.
                if (segmentsToBeMerged.size() > 1) {
                    break;
                }
                else { // if only one segment is found then remove the earlier one in list and put this.
                    // reset the total length to the current identified segment.
                    segmentsToBeMerged = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                    segmentsToBeMerged.add(segment);
                    totalLength = sizeOfOneSegmentAcrossPartition;
                }
            }
        }
        return segmentsToBeMerged;
    }

    private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSegCount(
            List<LoadMetadataDetails> listOfSegmentsAfterPreserve, List<String> segsToBeSkipped, long minorVacuumSegCount)
    {
        List<LoadMetadataDetails> unMergedSegments =
                new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        // this value is brought in from the config file, populated by the user
        int level1Size = -1;
        boolean isUserDefinedMinorSegCountUsed = false;

        if (minorVacuumSegCount >= 2) {
            level1Size = (int) minorVacuumSegCount;
            isUserDefinedMinorSegCountUsed = true;
        }

        int unMergeCounter = 0;

        // check size of each segment , sum it up across partitions
        for (LoadMetadataDetails segment : listOfSegmentsAfterPreserve) {
            String segName = segment.getLoadName();
            if (segsToBeSkipped.contains(segName)) {
                continue;
            }
            unMergeCounter++;
            unMergedSegments.add(segment);
            if (isUserDefinedMinorSegCountUsed) {
                if (unMergeCounter == (level1Size)) {
                    break;
                }
            }
        }

        if (isUserDefinedMinorSegCountUsed) {
            if (unMergedSegments.size() != 0 && unMergedSegments.size() == minorVacuumSegCount) {
                return unMergedSegments;
            }
            else {
                return new ArrayList<>(0);
            }
        }
        else {
            if (unMergedSegments.size() != 0) {
                return unMergedSegments;
            }
            else {
                return new ArrayList<>(0);
            }
        }
    }

    private static long getSizeOfSegment(String tablePath, String segId, Configuration configuration)
    {
        String loadPath = CarbonTablePath.getSegmentPath(tablePath, segId);
        CarbonFile segmentFolder =
                FileFactory.getCarbonFile(loadPath, configuration);
        return getSizeOfFactFileInLoad(segmentFolder);
    }

    private static long getSizeOfFactFileInLoad(CarbonFile carbonFile)
    {
        long factSize = 0;

        // carbon data file case.
        CarbonFile[] factFile = carbonFile.listFiles(file -> CarbonTablePath.isCarbonDataFile(file.getName()));

        for (CarbonFile fact : factFile) {
            factSize += fact.getSize();
        }
        return factSize;
    }
}
