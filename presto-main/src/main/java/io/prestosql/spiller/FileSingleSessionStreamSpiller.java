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
package io.prestosql.spiller;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Stopwatch;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.transport.execution.buffer.PagesSerdeUtil.writeSerializedPage;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class FileSingleSessionStreamSpiller
{
    private SliceOutput output;
    private OutputStream outputStream;
    private FileHolder sessionTargetFile;
    private boolean useSessionDirectSerde;
    private boolean closed;

    void setTargetFile(FileHolder targetFile, boolean useDirectSerde)
    {
        this.sessionTargetFile = targetFile;
        this.useSessionDirectSerde = useDirectSerde;
    }

    void setOutput(SliceOutput output)
    {
        this.output = output;
    }

    void setOutputStream(OutputStream outputStream)
    {
        this.outputStream = outputStream;
    }

    void closeSession()
    {
        if (!closed && (output != null || outputStream != null)) {
            try {
                if (useSessionDirectSerde) {
                    checkState(outputStream != null, "output stream is null");
                    outputStream.close();
                    outputStream = null;
                }
                else {
                    checkState(output != null, "output is null");
                    output.close();
                    output = null;
                }
                closed = true;
            }
            catch (UncheckedIOException | IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to close outputStream", e);
            }
        }
    }

    protected Stats writePages(boolean writable, Iterator<Page> pageIterator, PagesSerde serde)
    {
        long totalSpilledPagesInMemorySize = 0;
        long totalBytesWritten = 0;
        long totalSpilledBytes = 0;
        long totalWriteTime;
        List<Long> pageSizesList = new LinkedList<>();

        synchronized (output) {
            checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
            Stopwatch timer = Stopwatch.createStarted();
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                totalSpilledPagesInMemorySize += page.getSizeInBytes();
                SerializedPage serializedPage = serde.serialize(page);
                long pageSize = serializedPage.getSizeInBytes();
                totalBytesWritten += pageSize;
                totalSpilledBytes += pageSize;
                pageSizesList.add(pageSize);
                writeSerializedPage(output, serializedPage);
            }
            try {
                output.flush();
            }
            catch (IOException ioException) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to flush output", ioException);
            }
            timer.stop();
            totalWriteTime = timer.elapsed(TimeUnit.MILLISECONDS);
            return new Stats(totalSpilledPagesInMemorySize, totalBytesWritten, totalSpilledBytes, totalWriteTime, pageSizesList);
        }
    }

    protected Stats writePagesDirect(boolean writable, Iterator<Page> pageIterator, PagesSerde serde)
    {
        long totalSpilledPagesInMemorySize = 0;
        long totalBytesWritten = 0;
        long totalSpilledBytes = 0;
        long totalWriteTime;
        List<Long> pageSizesList = new LinkedList<>();

        synchronized (outputStream) {
            checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
            Stopwatch timer = Stopwatch.createStarted();
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                long pageSize = page.getSizeInBytes();
                totalSpilledPagesInMemorySize += pageSize;
                totalBytesWritten += pageSize;
                totalSpilledBytes += pageSize;
                pageSizesList.add(pageSize);
                serde.serialize(outputStream, page);
            }
            try {
                outputStream.flush();
            }
            catch (IOException ioException) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to flush outputStream", ioException);
            }
            timer.stop();
            totalWriteTime = timer.elapsed(TimeUnit.MILLISECONDS);
            return new Stats(totalSpilledPagesInMemorySize, totalBytesWritten, totalSpilledBytes, totalWriteTime, pageSizesList);
        }
    }

    long getTargetFileSize()
    {
        if (useSessionDirectSerde) {
            try {
                outputStream.flush();
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to flush outputStream", e);
            }
            if (outputStream instanceof Output) {
                return ((Output) outputStream).position();
            }
            else {
                return ((OutputStreamSliceOutput) outputStream).size();
            }
        }
        else {
            try {
                output.flush();
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to flush output", e);
            }
            return output.size();
        }
    }

    protected class Stats
    {
        private final long totalSpilledPagesInMemorySize;
        private final long totalBytesWritten;
        private final long totalSpilledBytes;
        private final long totalWriteTime;
        private final List<Long> pageSizesList;

        Stats(long totalSpilledPagesInMemorySize, long totalBytesWritten, long totalSpilledBytes, long totalWriteTime, List<Long> pageSizesList)
        {
            this.totalSpilledPagesInMemorySize = totalSpilledPagesInMemorySize;
            this.totalBytesWritten = totalBytesWritten;
            this.totalSpilledBytes = totalSpilledBytes;
            this.totalWriteTime = totalWriteTime;
            this.pageSizesList = pageSizesList;
        }

        public long getTotalSpilledPagesInMemorySize()
        {
            return totalSpilledPagesInMemorySize;
        }

        public long getTotalBytesWritten()
        {
            return totalBytesWritten;
        }

        public long getTotalSpilledBytes()
        {
            return totalSpilledBytes;
        }

        public long getTotalWriteTime()
        {
            return totalWriteTime;
        }

        public List<Long> getPageSizesList()
        {
            return pageSizesList;
        }
    }
}
