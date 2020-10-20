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
package io.prestosql.spi.heuristicindex;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface IndexWriter
{
    /**
     * Add values to the writer cache. The indices are not written to the specified filesystem until persist() is called
     *
     * @param values values to be indexed
     * @param connectorMetadata metadata for the index
     * @throws IOException thrown during index creation
     */
    void addData(Map<String, List<Object>> values, Properties connectorMetadata)
            throws IOException;

    void persist()
            throws IOException;
}
