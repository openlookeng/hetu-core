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
package io.hetu.core.plugin.hbase.utils;

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
     * DEFAULT
     */
    public static final String DEFAULT = "default";

    /**
     * ARRAY
     */
    public static final String ARRAY = "ARRYA [ ";

    /**
     * apostrophe
     */
    public static final String APOSTROPHE = "'";

    /**
     * 2
     */
    public static final int NUMBER2 = 2;

    /**
     * 3
     */
    public static final int NUMBER3 = 3;

    /**
     * 4
     */
    public static final int NUMBER4 = 4;

    /**
     * 1024
     */
    public static final int NUMBER1024 = 1024;

    /**
     * -1
     */
    public static final int NUMBER_NEGATIVE_1 = -1;

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
    public static final int SCAN_CACHING_SIZE = 5000;

    /**
     * constant string
     */
    public static final String S_SCHEMA = "schema";

    /**
     * constant string
     */
    public static final String S_TABLE = "table";

    /**
     * constant string
     */
    public static final String S_COLUMNS = "columns";

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
    public static final String S_NAME = "name";

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
    public static final String S_TYPE = "type";

    /**
     * constant string
     */
    public static final String S_ORDINAL = "ordinal";

    /**
     * constant string
     */
    public static final String S_COMMENT = "comment";

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
    public static final String S_FLASE = "false";

    /**
     * constant string
     */
    public static final String S_NULL = "null";

    /**
     * hdfs authentication kerberos
     */
    public static final String HDFS_AUTHENTICATION_KERBEROS = "KERBEROS";

    private Constants() {}
}
