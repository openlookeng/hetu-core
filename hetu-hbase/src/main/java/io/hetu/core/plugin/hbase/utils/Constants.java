/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.utils;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * constants
 *
 * @since 2020-03-30
 */
public class Constants
{
    /**
     * BATCHGET_SPLIT_RECORD_COUNT
     */
    public static final int BATCHGET_SPLIT_RECORD_COUNT = 20;

    /**
     * BATCHGET_SPLIT_MAX_COUNT
     */
    public static final int BATCHGET_SPLIT_MAX_COUNT = 30;

    /**
     * POINT = '.'
     */
    public static final String POINT = ".";

    /**
     * SPLITPOINT = '\\.'
     */
    public static final String SPLITPOINT = "\\.";

    /**
     * SEPARATOR
     */
    public static final String SEPARATOR = ":";

    /**
     * SEPARATOR_START_END_KEY
     */
    public static final String SEPARATOR_START_END_KEY = "~";

    /**
     * START_END_ROW_KEYS_COUNT
     */
    public static final int START_END_KEYS_COUNT = 100;

    /**
     * ROWKEY_TAIL
     */
    public static final String ROWKEY_TAIL = "|";

    /**
     * HBASE_ROWID_NAME
     */
    public static final String HBASE_ROWID_NAME = "$rowId";

    /**
     * DEFAULT
     */
    public static final String DEFAULT = "default";

    /**
     * 2
     */
    public static final int NUMBER2 = 2;

    /**
     * 3
     */
    public static final int NUMBER3 = 3;

    /**
     * "no such transaction: %s"
     */
    public static final String ERROR_MESSAGE_TEMPLATE = "no such transaction: %s";

    /**
     * 0x0104_0000
     */
    public static final int NUNBER_0X0104_0000 = 0x0104_0000;

    /**
     * SCAN_CACHING_SIZE
     */
    public static final int SCAN_CACHING_SIZE = 10000;

    /**
     * PUT_BATCH_SIZE
     */
    public static final int PUT_BATCH_SIZE = 10000;

    /**
     * constant string
     */
    public static final String S_ROWID = "rowId";

    /**
     * constant string
     */
    public static final String S_EXTERNAL = "external";

    /**
     * constant string
     */
    public static final String S_SERIALIZER_CLASS_NAME = "serializerClassName";

    /**
     * constant string
     */
    public static final String S_INDEX_COLUMNS = "indexColumns";

    /**
     * constant string
     */
    public static final String S_HBASE_TABLE_NAME = "hbaseTableName";

    /**
     * constant string
     */
    public static final String S_SPLIT_BY_CHAR = "splitByChar";

    /**
     * constant string
     */
    public static final String S_FAMILY = "family";

    /**
     * constant string
     */
    public static final String S_QUALIFIER = "qualifier";

    /**
     * constant string
     */
    public static final String S_ORDINAL = "ordinal";

    /**
     * constant string
     */
    public static final String S_INDEXED = "indexed";

    /**
     * constant string
     */
    public static final String S_TRUE = "true";

    /**
     * constant string
     */
    public static final String S_NULL = "null";

    /**
     * hdfs authentication kerberos
     */
    public static final String HDFS_AUTHENTICATION_KERBEROS = "KERBEROS";

    /**
     * type class names' list
     */
    public static final List<String> HBASE_DATA_TYPE_NAME_LIST = Collections.unmodifiableList(new ArrayList<String>() {
        {
            this.add(VarcharType.class.getName());
            this.add(TinyintType.class.getName());
            this.add(SmallintType.class.getName());
            this.add(IntegerType.class.getName());
            this.add(BigintType.class.getName());
            this.add(DoubleType.class.getName());
            this.add(BooleanType.class.getName());
            this.add(TimeType.class.getName());
            this.add(DateType.class.getName());
            this.add(TimestampType.class.getName());
        }
    });

    private Constants() {}
}
