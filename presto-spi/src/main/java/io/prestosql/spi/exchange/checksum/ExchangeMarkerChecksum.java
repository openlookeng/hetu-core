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

import com.google.common.hash.Hasher;
import io.prestosql.spi.checksum.Checksum;

public class ExchangeMarkerChecksum
        implements Checksum
{
    private final Hasher hasher;

    public ExchangeMarkerChecksum(Hasher hasher)
    {
        this.hasher = hasher;
    }

    @Override
    public String digest(byte[] data)
    {
        return hasher.putBytes(data).hash().toString();
    }

    @Override
    public String digest()
    {
        return hasher.hash().toString();
    }

    @Override
    public void update(byte[] data)
    {
        hasher.putBytes(data);
    }
}
