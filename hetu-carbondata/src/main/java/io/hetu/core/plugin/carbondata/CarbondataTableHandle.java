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
package io.hetu.core.plugin.carbondata;

import io.prestosql.plugin.hive.HiveBucketHandle;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveTableHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CarbondataTableHandle
        extends HiveTableHandle
{
    public CarbondataTableHandle(String schemaName, String tableName, Map<String, String> tableParameters, List<HiveColumnHandle> partitionColumns, Optional<HiveBucketHandle> bucketHandle)
    {
        super(schemaName, tableName, tableParameters, partitionColumns, bucketHandle);
    }

    @Override
    public boolean isDeleteAsInsertSupported()
    {
        return true;
    }

    /* This method checks if reuse table scan can be used*/
    @Override
    public boolean isReuseTableScanSupported()
    {
        return false;
    }

    @Override
    public boolean isUpdateAsInsertSupported()
    {
        return true;
    }
}
