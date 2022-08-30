/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.spi.exchange.marker;

import io.prestosql.spi.checksum.CheckSumAlgorithm;
import io.prestosql.spi.checksum.ChecksumFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class HetuFileSystemExchangeMarkerFactory
        implements ExchangeMarkerFactory
{
    private final AtomicInteger markerId;
    private final ChecksumFactory checksumFactory;

    public HetuFileSystemExchangeMarkerFactory(ChecksumFactory checksumFactory)
    {
        markerId = new AtomicInteger(0);
        this.checksumFactory = checksumFactory;
    }

    @Override
    public ExchangeMarker create(String taskId, long offset, CheckSumAlgorithm algorithm)
    {
        int id = markerId.getAndAdd(1);
        return new HetuFileSystemExchangeMarker(id, taskId, offset, checksumFactory.create(algorithm));
    }
}
