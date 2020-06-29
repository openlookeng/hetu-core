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
package io.prestosql.spi.metastore;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;

/**
 * hetu error code
 *
 * @since 2020-02-27
 */
public enum HetuErrorCode
        implements ErrorCodeSupplier
{
    /**
     * hetu metastore code
     */
    HETU_METASTORE_CODE(0, EXTERNAL);
    private static final int BASE_CODE = 0x0400_0000;
    private final ErrorCode errorCode;

    HetuErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + BASE_CODE, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
