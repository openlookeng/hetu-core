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
package io.hetu.core.plugin.hbase.conf;

import com.google.common.collect.ImmutableList;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.prestosql.spi.session.PropertyMetadata;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.stringProperty;

/**
 * Class contains all column properties for the HBase table. Used when add a column:
 * <p>
 * ALTER TABLE tableName ADD COLUMN
 * column_name INTEGER with(family='f',qualifier='q');
 *
 * @since 2020-03-30
 */
public class HBaseColumnProperties
{
    private static final HBaseColumnProperties HBASE_COLUMN_PROPERTIES = new HBaseColumnProperties();

    private final List<PropertyMetadata<?>> columnProperties;

    private HBaseColumnProperties()
    {
        PropertyMetadata<String> s1 =
                stringProperty(Constants.S_FAMILY,
                        "Hetu add column that maps to the HBase column's family.", null, false);
        PropertyMetadata<String> s2 =
                stringProperty(Constants.S_QUALIFIER,
                        "Hetu add column that maps to the HBase column's qualifier.", null, false);
        columnProperties = ImmutableList.of(s1, s2);
    }

    /**
     * getColumnProperties
     *
     * @return PropertyMetadata list
     */
    public static List<PropertyMetadata<?>> getColumnProperties()
    {
        return HBASE_COLUMN_PROPERTIES.columnProperties;
    }

    /**
     * getInstance
     *
     * @return HBaseColumnProperties
     */
    public static HBaseColumnProperties getInstance()
    {
        return HBASE_COLUMN_PROPERTIES;
    }
}
