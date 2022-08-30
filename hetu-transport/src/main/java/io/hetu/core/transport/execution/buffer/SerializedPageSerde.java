/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.Slices;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

import static io.hetu.core.transport.execution.buffer.PageCodecMarker.MarkerSet.fromByteValue;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SerializedPageSerde
{
    private SerializedPageSerde()
    {
    }

    private static final Serializer<SerializedPage> serializer = new Serializer<SerializedPage>()
    {
        @Override
        public void write(Kryo kryo, Output output, SerializedPage page)
        {
            output.writeInt(page.getPositionCount());
            output.writeByte(page.getPageCodecMarkers());
            output.writeInt(page.getUncompressedSizeInBytes());
            output.writeInt(page.getSizeInBytes());
            output.writeBytes(page.getSliceArray());

            if (page.getPageMetadata().size() != 0) {
                String pageProperties = page.getPageMetadata().toString();
                byte[] propertiesByte = pageProperties
                        .replaceAll(",", System.lineSeparator())
                        .substring(1, pageProperties.length() - 1)
                        .getBytes(UTF_8);
                output.writeInt(propertiesByte.length);
                output.writeBytes(propertiesByte);
            }
            else {
                output.writeInt(0);
            }
        }

        @Override
        public SerializedPage read(Kryo kryo, Input input, Class<? extends SerializedPage> type)
        {
            int positionCount = input.readInt();
            byte pageCodecMarkers = input.readByte();
            int uncompressedSizeInBytes = input.readInt();
            int sliceLength = input.readInt();
            byte[] sliceBytes = input.readBytes(sliceLength);
            int pageMetadataBytesLength = input.readInt();
            if (pageMetadataBytesLength > 0) {
                byte[] pageMetadataBytes = input.readBytes(pageMetadataBytesLength);
                Properties pageMetadata = new Properties();
                try {
                    pageMetadata.load(new ByteArrayInputStream(pageMetadataBytes));
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return new SerializedPage(Slices.wrappedBuffer(sliceBytes),
                        fromByteValue(pageCodecMarkers),
                        positionCount,
                        uncompressedSizeInBytes,
                        pageMetadata);
            }
            return new SerializedPage(sliceBytes,
                    pageCodecMarkers,
                    positionCount,
                    uncompressedSizeInBytes);
        }
    };

    public static void serialize(OutputStream output, SerializedPage page)
    {
        serializer.write(null, (Output) output, page);
    }

    public static SerializedPage deserialize(InputStream input)
    {
        return serializer.read(null, (Input) input, SerializedPage.class);
    }

    public static void serialize(Kryo kryo, OutputStream output, SerializedPage page)
    {
        serializer.write(kryo, (Output) output, page);
    }

    public static SerializedPage deserialize(Kryo kryo, InputStream input)
    {
        return serializer.read(kryo, (Input) input, SerializedPage.class);
    }
}
