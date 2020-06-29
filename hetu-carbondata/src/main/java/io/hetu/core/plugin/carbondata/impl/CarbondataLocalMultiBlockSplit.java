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
package io.hetu.core.plugin.carbondata.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;

import java.util.List;
import java.util.stream.Collectors;

/**
 * CarbonLocalInputSplit represents a block, it contains a set of blocklet.
 */
public class CarbondataLocalMultiBlockSplit
{
    private static final long serialVersionUID = 3520344046772190207L;

    /*
     * Splits (HDFS Blocks) for task to scan.
     */
    private List<CarbondataLocalInputSplit> splitList;

    /*
     * The locations of all wrapped splits
     */
    private String[] locations;

    private FileFormat fileFormat = FileFormat.COLUMNAR_V3;

    private long length;

    @JsonCreator
    public CarbondataLocalMultiBlockSplit(
            @JsonProperty("splitList") List<CarbondataLocalInputSplit> splitList,
            @JsonProperty("locations") String[] locations)
    {
        this.splitList = splitList;
        this.locations = locations;
        if (!splitList.isEmpty()) {
            this.fileFormat = splitList.get(0).getFileFormat();
        }
    }

    public static CarbonMultiBlockSplit convertSplit(String multiSplitJson)
    {
        Gson gson = new Gson();
        CarbondataLocalMultiBlockSplit carbonLocalMultiBlockSplit =
                gson.fromJson(multiSplitJson, CarbondataLocalMultiBlockSplit.class);
        List<CarbonInputSplit> carbonInputSplitList =
                carbonLocalMultiBlockSplit.getSplitList().stream().map(CarbondataLocalInputSplit::convertSplit)
                        .collect(Collectors.toList());

        CarbonMultiBlockSplit carbonMultiBlockSplit =
                new CarbonMultiBlockSplit(carbonInputSplitList, carbonLocalMultiBlockSplit.getLocations());
        carbonMultiBlockSplit.setFileFormat(carbonLocalMultiBlockSplit.getFileFormat());

        return carbonMultiBlockSplit;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public String[] getLocations()
    {
        return locations;
    }

    @JsonProperty
    public List<CarbondataLocalInputSplit> getSplitList()
    {
        return splitList;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    public String getJsonString()
    {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
