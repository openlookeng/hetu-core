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
package io.prestosql.spi.exchange.marker;

import io.airlift.slice.Slice;
import io.prestosql.spi.checksum.Checksum;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HetuFileSystemExchangeMarker
        implements ExchangeMarker
{
    private final int id;
    private final long offset; // marker position in main file
    private int pageCount;
    private int rowCount;
    private long sizeInBytes;
    private final Checksum checksumGenerator;
    private final String taskId;
    private String checksum;

    public HetuFileSystemExchangeMarker(int markerId, String taskId, long offset, Checksum checksumGenerator)
    {
        checkArgument(markerId >= 0, "markerId is negative");
        checkArgument(offset >= 0, "offset is negative");
        this.taskId = taskId;
        this.checksumGenerator = checksumGenerator;
        this.id = markerId;
        this.offset = offset;
        this.rowCount = 0;
        this.sizeInBytes = 0;
        this.pageCount = 0;
        this.checksum = "";
    }

    public HetuFileSystemExchangeMarker(
            int id,
            String taskId,
            int pageCount,
            int rowCount,
            long offset,
            long sizeInBytes,
            String checksum)
    {
        this.id = id;
        this.taskId = taskId;
        this.offset = offset;
        this.pageCount = pageCount;
        this.rowCount = rowCount;
        this.sizeInBytes = sizeInBytes;
        this.checksum = checksum;
        checksumGenerator = null;
    }

    @Override
    public void addPage(Slice page, int rowCount)
    {
        if (checksumGenerator == null) {
            throw new IllegalStateException("read only marker");
        }
        this.rowCount += rowCount;
        sizeInBytes += page.length();
        pageCount += 1;
        checksumGenerator.update(page.getBytes());
    }

    @Override
    public void calculateChecksum()
    {
        this.checksum = checksumGenerator.digest();
    }

    @Override
    public int getId()
    {
        return id;
    }

    @Override
    public String getTaskId()
    {
        return taskId;
    }

    @Override
    public long getOffset()
    {
        return offset;
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public String getChecksum()
    {
        return checksum;
    }

    @Override
    public int getPageCount()
    {
        return pageCount;
    }

    @Override
    public int calculateSerializationSizeInBytes()
    {
        return Integer.BYTES                  // id
                + Integer.BYTES               // page count
                + Integer.BYTES               // row count
                + Long.BYTES                  // offset
                + Long.BYTES                  // size in bytes
                + Integer.BYTES               // size of taskid
                + taskId.getBytes(UTF_8).length    // taskid
                + Integer.BYTES               // size of checksum
                + checksum.getBytes(UTF_8).length; // checksum
    }

    @Override
    public byte[] serialize()
    {
        final int size = calculateSerializationSizeInBytes();
        byte[] taskIdBytes = taskId.getBytes(UTF_8);
        byte[] checksumBytes = checksum.getBytes(UTF_8);
        return ByteBuffer.allocate(size)
                .putInt(id)
                .putInt(pageCount)
                .putInt(rowCount)
                .putLong(offset)
                .putLong(sizeInBytes)
                .putInt(taskIdBytes.length)
                .put(taskIdBytes)
                .putInt(checksumBytes.length)
                .put(checksumBytes)
                .array();
    }

    public static ExchangeMarker deserialize(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int markerId = buffer.getInt();
        int markerPageCount = buffer.getInt();
        int markerRowCount = buffer.getInt();
        long markerOffset = buffer.getLong();
        long markerSizeInBytes = buffer.getLong();
        int taskIdLength = buffer.getInt();
        byte[] taskIdBytes = new byte[taskIdLength];
        buffer.get(taskIdBytes);
        String markerTaskId = new String(taskIdBytes, UTF_8);
        int checksumLength = buffer.getInt();
        byte[] checksumBytes = new byte[checksumLength];
        buffer.get(checksumBytes);
        String markerChecksum = new String(checksumBytes, UTF_8);
        return new HetuFileSystemExchangeMarker(
                markerId,
                markerTaskId,
                markerPageCount,
                markerRowCount,
                markerOffset,
                markerSizeInBytes,
                markerChecksum);
    }

    @Override
    public String toString()
    {
        return "{" +
                "id=" + id +
                ", offset=" + offset +
                ", pageCount=" + pageCount +
                ", rowCount=" + rowCount +
                ", sizeInBytes=" + sizeInBytes +
                ", checksum='" + checksum + '\'' +
                '}';
    }
}
