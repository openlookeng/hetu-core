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
package io.hetu.core.plugin.hbase.connector;

import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseColumnHandle
 *
 * @since 2020-03-20
 */
public class TestHBaseColumnHandle
{
    private static boolean indexed;
    private Optional<String> family = Optional.of("family");
    private Optional<String> qualifier = Optional.of("qualifier");
    private Type type = VarcharType.createVarcharType(30);
    private int ordinal;
    private String comment = "hbase column comment";
    private String name = "column1";
    private HBaseColumnHandle columnHandle;

    /**
     * testCreateHBaseColumnHandler
     */
    @Test
    public void testCreateHBaseColumnHandler()
    {
        columnHandle = new HBaseColumnHandle(name, family, qualifier, type, ordinal, comment, indexed);
    }

    /**
     * testMethod
     */
    @Test
    public void testMethod()
    {
        testCreateHBaseColumnHandler();
        assertEquals(name, columnHandle.getName());
        assertEquals(family, columnHandle.getFamily());
        assertEquals(qualifier, columnHandle.getQualifier());
        assertEquals(type, columnHandle.getType());
        assertEquals(ordinal, columnHandle.getOrdinal());
        assertEquals(comment, columnHandle.getComment());
        assertEquals(indexed, columnHandle.isIndexed());
    }

    /**
     * testEqauls
     */
    @Test
    public void testEqauls()
    {
        if (columnHandle == null) {
            testCreateHBaseColumnHandler();
        }
        HBaseColumnHandle columnHandle2 =
                new HBaseColumnHandle(name, family, qualifier, type, ordinal, comment, indexed);
        assertEquals(true, columnHandle.equals(columnHandle2));
        assertEquals(0, columnHandle.compareTo(columnHandle2));
    }

    /**
     * testOnlyRun
     */
    @Test
    public void testOnlyRun()
            throws Exception
    {
        if (columnHandle == null) {
            testCreateHBaseColumnHandler();
        }
        this.columnHandle.toString();
        this.columnHandle.parseToJson();
        this.columnHandle.hashCode();
        this.columnHandle.getColumnMetadata();
        this.columnHandle.equals(this.columnHandle);
        this.columnHandle.equals(null);
    }
}
