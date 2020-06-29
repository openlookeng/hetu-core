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
package io.prestosql;

import io.prestosql.spi.PrestoException;
import io.prestosql.transaction.TransactionId;

import static io.prestosql.spi.StandardErrorCode.UNKNOWN_TRANSACTION;
import static java.lang.String.format;

/**
 * Exception indicates the query is not in local transaction table
 * @since 2020-03-04
 */
public class NotInLocalTransactionException
        extends PrestoException
{
    public NotInLocalTransactionException()
    {
        super(UNKNOWN_TRANSACTION, "Not in a local transaction");
    }

    public NotInLocalTransactionException(TransactionId transactionId)
    {
        super(UNKNOWN_TRANSACTION, format("Unknown transaction ID locally: %s. Possibly expired? Commands ignored until end of transaction block", transactionId));
    }
}
