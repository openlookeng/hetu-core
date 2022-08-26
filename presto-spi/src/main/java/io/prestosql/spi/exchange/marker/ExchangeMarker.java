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

import io.airlift.slice.Slice;

public interface ExchangeMarker
{
    void addPage(Slice page, int rowCount);

    void calculateChecksum();

    int getPageCount();

    long getSizeInBytes();

    int getId();

    String getTaskId();

    long getOffset();

    int getRowCount();

    String getChecksum();

    int calculateSerializationSizeInBytes();

    byte[] serialize();
}
