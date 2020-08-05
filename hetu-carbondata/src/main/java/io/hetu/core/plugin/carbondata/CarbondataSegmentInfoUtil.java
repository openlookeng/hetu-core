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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CarbondataSegmentInfoUtil
{
    private final String destinationSegment;
    private final Set<String> sourceSegmentIdSet;

    @JsonCreator
    public CarbondataSegmentInfoUtil(
            @JsonProperty("destinationSegment") String destinationSegment,
            @JsonProperty("sourceSegmentIdSet") Set<String> sourceSegmentIdSet)
    {
        this.destinationSegment = requireNonNull(destinationSegment, "destinationSegment is null");
        this.sourceSegmentIdSet = requireNonNull(sourceSegmentIdSet, "sourceSegmentIdSet is null");
    }

    @JsonProperty
    public String getDestinationSegment()
    {
        return destinationSegment;
    }

    @JsonProperty
    public Set<String> getSourceSegmentIdSet()
    {
        return sourceSegmentIdSet;
    }
}
