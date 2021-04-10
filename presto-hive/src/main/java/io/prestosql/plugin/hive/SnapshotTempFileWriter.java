/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.airlift.log.Logger;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.plugin.hive.util.TempFileWriter;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;

public class SnapshotTempFileWriter
        implements HiveFileWriter
{
    private static final Logger log = Logger.get(SnapshotTempFileWriter.class);
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SnapshotTempFileWriter.class).instanceSize();

    private final TempFileWriter writer;

    public SnapshotTempFileWriter(
            OrcDataSink dataSink,
            List<Type> types)
    {
        writer = new TempFileWriter(types, dataSink);
    }

    @Override
    public long getWrittenBytes()
    {
        return writer.getWrittenBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + writer.getRetainedBytes();
    }

    @Override
    public void appendRows(Page page)
    {
        writer.writePage(page);
    }

    @Override
    public void commit()
    {
        try {
            writer.close();
        }
        catch (IOException | UncheckedIOException e) {
            // DO NOT delete the file. A newly schedule task may be recreating this file.
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
    }

    @Override
    public void rollback()
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            // DO NOT delete the file. A newly schedule task may be recreating this file.
            // Don't need to throw the exception either. This is part of the cancel-to-resume task.
            log.debug(e, "Error rolling back write to Hive");
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", writer)
                .toString();
    }
}
