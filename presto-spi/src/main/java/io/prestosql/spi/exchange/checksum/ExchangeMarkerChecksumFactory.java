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
package io.prestosql.spi.exchange.checksum;

import com.google.common.hash.Hashing;
import io.prestosql.spi.checksum.CheckSumAlgorithm;
import io.prestosql.spi.checksum.Checksum;
import io.prestosql.spi.checksum.ChecksumFactory;

public class ExchangeMarkerChecksumFactory
        implements ChecksumFactory
{
    public Checksum create(CheckSumAlgorithm algorithm)
    {
        switch (algorithm) {
            case MD5:
                return new ExchangeMarkerChecksum(Hashing.md5().newHasher());
            case SHA256:
                return new ExchangeMarkerChecksum(Hashing.sha256().newHasher());
            case SHA512:
                return new ExchangeMarkerChecksum(Hashing.sha512().newHasher());
            default:
                return new ExchangeMarkerChecksum(Hashing.murmur3_128().newHasher());
        }
    }
}
