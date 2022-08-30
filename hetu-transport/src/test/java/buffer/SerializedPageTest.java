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
package buffer;

import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.checksum.CheckSumAlgorithm;
import io.prestosql.spi.exchange.checksum.ExchangeMarkerChecksumFactory;
import io.prestosql.spi.exchange.marker.HetuFileSystemExchangeMarkerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SerializedPageTest
{
    @Test
    public void testForExchangeMarker()
    {
        HetuFileSystemExchangeMarkerFactory markerFactory = new HetuFileSystemExchangeMarkerFactory(new ExchangeMarkerChecksumFactory());
        SerializedPage markerPage = SerializedPage.forExchangeMarker(markerFactory.create("query.0.0.0", 0, CheckSumAlgorithm.MURMUR3));
        assertTrue(markerPage.isExchangeMarkerPage());
        assertFalse(markerPage.isMarkerPage());
    }
}
