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
package io.hetu.core.plugin.hbase.query;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * HBaseUpdatablePageSource
 *
 * @since 2020-03-18
 */
public class HBaseUpdatablePageSource
        implements UpdatablePageSource
{
    private static final Logger LOG = Logger.get(HBaseUpdatablePageSource.class);

    private final HBaseConnection hBaseConnection;

    private final RecordPageSource inner;

    private final HBaseRecordSet recordSet;

    public HBaseUpdatablePageSource(HBaseRecordSet recordSet)
    {
        this.recordSet = recordSet;
        this.inner = new RecordPageSource(recordSet);
        this.hBaseConnection = recordSet.getHBaseConnection();
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        TableName tableName =
                TableName.valueOf(
                        recordSet.getHBaseTableHandle().getHbaseTableName().get());
        Optional<Type> rowIdType =
                ((HBaseRecordCursor) (inner.getCursor()))
                        .columnHandles.stream()
                        .filter(col -> col.getName().equals(recordSet.getHBaseTableHandle().getRowId()))
                        .map(HBaseColumnHandle::getType)
                        .findAny();
        StringRowSerializer serializer = new StringRowSerializer();

        try (Table table = hBaseConnection.getConn().getTable(tableName)) {
            List<Delete> deletes = new ArrayList<>();
            Delete delete;
            for (int index = 0; index < rowIds.getPositionCount(); index++) {
                Object value = TypeUtils.readNativeValue(rowIdType.get(), rowIds, index);
                byte[] byteValue = serializer.setObjectBytes(rowIdType.get(), value);
                delete = new Delete(byteValue);
                deletes.add(delete);
            }
            if (deletes.size() > 0) {
                table.delete(deletes);
            }
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }

    @Override
    public long getCompletedBytes()
    {
        return inner.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return inner.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return inner.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return inner.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return inner.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        inner.close();
    }
}
