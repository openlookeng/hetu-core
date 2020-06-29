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
package io.prestosql.client.protocol;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.prestosql.client.Column;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static io.prestosql.client.util.TypeUtil.parseType;
import static java.util.Objects.requireNonNull;

public class DataCenterRowIterable
        implements Iterable<List<Object>>
{
    private final ConnectorSession session;
    private final List<Column> columns;
    private final Page page;
    private final TypeManager typeManager;

    /**
     * Instantiates a new Data center row iterable.
     *
     * @param session the session
     * @param columns the columns
     * @param page the page
     */
    public DataCenterRowIterable(ConnectorSession session, List<Column> columns, Page page, TypeManager typeManager)
    {
        this.session = session;
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "types is null"));
        this.page = requireNonNull(page, "page is null");
        this.typeManager = typeManager;
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return new DataCenterRowIterator(session, columns, page, typeManager);
    }

    private static class DataCenterRowIterator
            extends AbstractIterator<List<Object>>
    {
        private final ConnectorSession session;
        private final List<Column> columns;
        private final Page page;
        private int position = -1;
        private TypeManager typeManager;

        private DataCenterRowIterator(ConnectorSession session, List<Column> columns, Page page, TypeManager typeManager)
        {
            this.session = session;
            this.columns = columns;
            this.page = page;
            this.typeManager = typeManager;
        }

        @Override
        protected List<Object> computeNext()
        {
            position++;
            if (position >= page.getPositionCount()) {
                return endOfData();
            }

            List<Object> values = new ArrayList<>(page.getChannelCount());
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                String typeStr = columns.get(channel).getType().toLowerCase(Locale.ENGLISH);
                Type type = parseType(typeManager, typeStr);
                Block block = page.getBlock(channel);
                values.add(type.getObjectValue(session, block, position));
            }
            return Collections.unmodifiableList(values);
        }
    }
}
