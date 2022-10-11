/*
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
package io.prestosql.exchange;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.USER_ERROR;

public enum FileSystemExchangeErrorCode
        implements ErrorCodeSupplier
{
    MAX_OUTPUT_PARTITION_COUNT_EXCEEDED(0, USER_ERROR),;
    private static final int BASE_CODE = 0x0510_0000;
    private final ErrorCode errorCode;

    FileSystemExchangeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + BASE_CODE, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
