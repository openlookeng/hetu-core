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

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;

/**
 * HBaseErrorCode
 *
 * @since 2020-03-30
 */
public enum HBaseErrorCode
        implements ErrorCodeSupplier
{
    /**
     * Thrown when an HBase error is caught that we were not expecting,
     * such as when a create table operation fails (even though we know it will succeed due to our validation steps)
     */
    UNEXPECTED_HBASE_ERROR(10, EXTERNAL),

    /**
     * Thrown when a ZooKeeper error is caught due to a failed operation
     */
    ZOOKEEPER_ERROR(11, EXTERNAL),

    /**
     * Thrown when a serialization error occurs when reading/writing data from/to HBase
     */
    IO_ERROR(12, EXTERNAL),

    /**
     * Thrown when a table that is expected to exist does not exist
     */
    HBASE_TABLE_DNE(13, EXTERNAL),

    /**
     * Thrown when a table that is not expected to exist, does exist
     */
    HBASE_TABLE_EXISTS(14, EXTERNAL),

    /**
     * Thrown when an attempt to start/stop MiniHBaseCluster fails (testing only)
     */
    MINI_HBASE(15, EXTERNAL),

    /**
     * Thrown when hbase metastore is not right
     */
    UNSUPPORTED_TYPE(16, EXTERNAL),

    /**
     * Thrown when a new create table specify external = true
     */
    HBASE_CREATE_ERROR(17, EXTERNAL);

    private final ErrorCode errorCode;

    HBaseErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + Constants.NUNBER_0X0104_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
