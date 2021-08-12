/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;
import static io.prestosql.spi.ErrorType.INTERNAL_ERROR;

public enum KylinErrorCode
        implements ErrorCodeSupplier
{
    KYLIN_COLUMN_ERROR(0, EXTERNAL),
    KYLIN_CUBE_NOT_READY(1, EXTERNAL),
    KYLIN_CUBE_NOT_FOUND(2, EXTERNAL),
    KYLIN_REST_ERROR(3, INTERNAL_ERROR),
    KYLIN_INVALID_CONNECTION_URL(4, INTERNAL_ERROR);
    private final ErrorCode errorCode;

    KylinErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0400_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return null;
    }
}
