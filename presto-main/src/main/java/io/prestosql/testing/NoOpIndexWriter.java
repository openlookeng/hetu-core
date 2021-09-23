/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.testing;

import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.IndexWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NoOpIndexWriter
        implements IndexWriter
{
    @Override
    public void addData(Map<String, List<Object>> values, Properties connectorMetadata)
    {
        throw new UnsupportedOperationException(HetuConstant.HINDEX_CONFIG_ERROR_MSG);
    }

    @Override
    public long persist()
            throws IOException
    {
        throw new UnsupportedOperationException(HetuConstant.HINDEX_CONFIG_ERROR_MSG);
    }
}
