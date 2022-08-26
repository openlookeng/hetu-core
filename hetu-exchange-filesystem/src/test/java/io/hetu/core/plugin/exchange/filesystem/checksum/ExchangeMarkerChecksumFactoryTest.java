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
package io.hetu.core.plugin.exchange.filesystem.checksum;

import io.prestosql.spi.checksum.Checksum;
import io.prestosql.spi.exchange.checksum.ExchangeMarkerChecksumFactory;
import org.testng.annotations.Test;

import static io.prestosql.spi.checksum.CheckSumAlgorithm.MURMUR3;
import static org.testng.Assert.assertEquals;

public class ExchangeMarkerChecksumFactoryTest
{
    @Test
    public void testCreate()
    {
        ExchangeMarkerChecksumFactory checksumFactory = new ExchangeMarkerChecksumFactory();
        Checksum checksum = checksumFactory.create(MURMUR3);
        checksum.update(new byte[]{1, 2, 3});
        checksum.update(new byte[]{4, 5, 6});
        checksum.update(new byte[]{7, 8, 9});
        Checksum checksum1 = checksumFactory.create(MURMUR3);
        assertEquals(checksum.digest(), checksum1.digest(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }
}
