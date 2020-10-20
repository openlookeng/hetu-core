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
package io.prestosql.spi.heuristicindex;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class TestIndexMetadata
{
    @Test
    public void test()
    {
        Index index = new Index()
        {
            @Override
            public String getId()
            {
                return null;
            }

            @Override
            public boolean addValues(Map<String, List<Object>> values)
            {
                return false;
            }

            @Override
            public boolean matches(Object expression)
            {
                return false;
            }

            @Override
            public void serialize(OutputStream os)
            {
            }

            @Override
            public Index deserialize(InputStream is)
            {
                return this;
            }

            @Override
            public Index intersect(Index another)
            {
                return null;
            }

            @Override
            public Index union(Index another)
            {
                return null;
            }

            @Override
            public Properties getProperties()
            {
                return null;
            }

            @Override
            public void setProperties(Properties properties)
            {
            }

            @Override
            public int getExpectedNumOfEntries()
            {
                return 0;
            }

            @Override
            public void setExpectedNumOfEntries(int expectedNumOfEntries)
            {
            }

            @Override
            public long getMemorySize()
            {
                return 0;
            }

            @Override
            public void setMemorySize(long memorySize)
            {
            }

            @Override
            public boolean supportMultiColumn()
            {
                return false;
            }
        };

        String table = "table";
        String[] columns = new String[] {"columns"};
        String rootUri = "/tmp";
        String uri = "foo";
        long splitStart = 10;
        long lastUpdated = 20;

        IndexMetadata indexMetadata = new IndexMetadata(
                index,
                table,
                columns,
                rootUri,
                uri,
                splitStart,
                lastUpdated);
        assertEquals(indexMetadata.getIndex(), index);
        assertEquals(indexMetadata.getTable(), table);
        assertEquals(indexMetadata.getColumns(), columns);
        assertEquals(indexMetadata.getRootUri(), rootUri);
        assertEquals(indexMetadata.getUri(), uri);
        assertEquals(indexMetadata.getSplitStart(), splitStart);
        assertEquals(indexMetadata.getLastModifiedTime(), lastUpdated);
    }
}
