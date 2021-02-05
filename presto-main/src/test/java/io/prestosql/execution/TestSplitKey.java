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
package io.prestosql.execution;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.MockSplit;
import io.prestosql.metadata.Split;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.block.TestingBlockJsonSerde;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestSplitKey
{
    MockSplit mockConnectorSplit1;
    MockSplit mockConnectorSplit2;
    MockSplit mockConnectorSplit3;

    Split split1;
    Split split2;
    Split split3;

    CatalogName catalogName = new CatalogName("bogus_catalog");
    String schema = "test_schema";
    String table = "test_table";

    @BeforeTest
    public void setup()
    {
        mockConnectorSplit1 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/a=20/000000_0", 0, 10, System.currentTimeMillis());

        Long lastModified = System.currentTimeMillis();
        mockConnectorSplit2 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/a=21/000000_1", 0, 10, lastModified);
        mockConnectorSplit3 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/b=22/000000_1", 11, 50, lastModified);

        split1 = new Split(catalogName, mockConnectorSplit1, Lifespan.taskWide());
        split2 = new Split(catalogName, mockConnectorSplit2, Lifespan.taskWide());
        split3 = new Split(catalogName, mockConnectorSplit3, Lifespan.taskWide());
    }

    @Test
    public void testGetCatalog()
    {
        SplitKey splitKey1 = new SplitKey(split1, catalogName.toString(), schema, table);
        assertEquals(splitKey1.getCatalog(), catalogName.toString(), "catalog name should be equal");
    }

    @Test
    public void testGetSchema()
    {
        SplitKey splitKey1 = new SplitKey(split1, catalogName.toString(), schema, table);
        assertEquals(splitKey1.getSchema(), schema, "schema name should be equal");
    }

    @Test
    public void testGetTable()
    {
        SplitKey splitKey1 = new SplitKey(split1, catalogName.toString(), schema, table);
        assertEquals(splitKey1.getTable(), table, "table name should be equal");
    }

    @Test
    public void testGetQualifiedTableName()
    {
        SplitKey splitKey1 = new SplitKey(split1, catalogName.toString(), schema, table);
        assertEquals(splitKey1.getQualifiedTableName(), QualifiedName.of(catalogName.getCatalogName(), schema, table), "catalog name should be equal");
    }

    @Test
    public void testEquals()
    {
        SplitKey splitKey1 = new SplitKey(split1, catalogName.toString(), schema, table);
        SplitKey splitKey2 = new SplitKey(split2, catalogName.toString(), schema, table);
        assertNotEquals(splitKey1, splitKey2, "split key should not be equal");

        SplitKey splitKey3 = new SplitKey(split3, catalogName.toString(), schema, table);
        assertNotEquals(splitKey2, splitKey3, "split key should not be equal");

        // Scenario where file is updated but the split path/start/end are the same
        MockSplit mockConnectorSplit4 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_0", 0, 10, System.currentTimeMillis());
        Split split4 = new Split(catalogName, mockConnectorSplit4, Lifespan.taskWide());
        SplitKey splitKey4 = new SplitKey(split4, catalogName.toString(), schema, table);
        // Should not be treated the same as the data may now be outdated
        assertNotEquals(splitKey1, splitKey4);
    }

    @Test
    public void testSerde() throws IOException
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new ObjectMapperProvider().get().readValue(jsonParser, TestingColumnHandle.class);
                            }
                        })
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde))
                        .addKeyDeserializer(SplitKey.class, new SplitKey.KeyDeserializer()));

        SplitKey splitkey = new SplitKey(split1, catalogName.toString(), schema, table);
        assertEquals(mapper.readerFor(SplitKey.class).readValue(mapper.writeValueAsString(splitkey)), splitkey);

        Map<SplitKey, String> splitWorkers = new HashMap<>();
        splitWorkers.put(splitkey, "worker1");
        String mapJson = mapper.writeValueAsString(splitWorkers);
        TypeReference<HashMap<SplitKey, String>> typeRef
                = new TypeReference<HashMap<SplitKey, String>>() {};
        assertEquals(mapper.readValue(mapJson, typeRef), splitWorkers);
    }
}
