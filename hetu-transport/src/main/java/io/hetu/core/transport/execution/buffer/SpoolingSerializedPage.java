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
package io.hetu.core.transport.execution.buffer;

import io.airlift.slice.Slice;

import java.util.Properties;

public class SpoolingSerializedPage
        extends SerializedPage
{
    private final long spoolingMarkerData;

    public SpoolingSerializedPage(Slice slice, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes, Properties pageMetadata, long spoolingMarkerData)
    {
        super(slice, markers, positionCount, uncompressedSizeInBytes, pageMetadata);
        this.spoolingMarkerData = spoolingMarkerData;
    }

    public long getSpoolingMarkerData()
    {
        return spoolingMarkerData;
    }
}
