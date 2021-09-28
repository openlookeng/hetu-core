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
package io.hetu.core.plugin.hbase.conf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.serializers.HBaseRowSerializer;
import io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.VarcharType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the HBase connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (hbase_table_name= 'namespace:table', column_mapping = 'b:md:b', external = true);
 *
 * @since 2020-03-30
 */
public final class HBaseTableProperties
{
    /**
     * column_mapping
     */
    public static final String COLUMN_MAPPING = "column_mapping";
    /**
     * index_columns
     */
    public static final String INDEX_COLUMNS = "index_columns";
    /**
     * external
     */
    public static final String EXTERNAL = "external";
    /**
     * locality_groups
     */
    public static final String LOCALITY_GROUPS = "locality_groups";
    /**
     * row_id
     */
    public static final String ROW_ID = "row_id";
    /**
     * serializer
     */
    public static final String SERIALIZER = "serializer";
    /**
     * hbase_table_name
     */
    public static final String HBASE_TABLE_NAME = "hbase_table_name";
    /**
     * split_by_char
     */
    public static final String SPLIT_BY_CHAR = "split_by_char";
    private static final String COLON_SPLITTER = ":";
    private static final String COMMA_SPLITTER = ",";
    private static final String PIPE_SPLITTER = "\\|";

    private final List<PropertyMetadata<?>> tableProperties;

    /**
     * constructor
     */
    @Inject
    public HBaseTableProperties()
    {
        PropertyMetadata<String> s1 =
                stringProperty(
                        COLUMN_MAPPING,
                        "HBase column metadata: col_name:col_family:col_qualifier,Not setting this property results in auto-generated column names."
                                + "for example : column_mapping=\"rowkey:f:rowkey,column1:family1:column1,column2:family1:column2\"",
                        null,
                        false);
        PropertyMetadata<String> s2 =
                stringProperty(
                        INDEX_COLUMNS,
                        "A comma-delimited list of Hetu columns that are indexed in this table's corresponding "
                                + "index table. Default is no indexed columns.",
                        "",
                        false);
        PropertyMetadata<Boolean> s3 =
                booleanProperty(
                        EXTERNAL,
                        "If true, Hetu will only do metadata operations for the table. Else, Hetu will create and "
                                + "drop HBase tables where appropriate. Default false.",
                        true,
                        false);
        PropertyMetadata<String> s4 =
                stringProperty(
                        LOCALITY_GROUPS,
                        "List of locality groups to set on the HBase table. Only valid on internal tables. String"
                                + " format is locality group name, colon, comma delimited list of Hetu column names in"
                                + " the group. Groups are delimited by pipes. Example:"
                                + " group1:colA,colB,colC|group2:colD,colE,colF|etc.... Default is no locality groups.",
                        null,
                        false);
        PropertyMetadata<String> s5 =
                stringProperty(
                        ROW_ID,
                        "Hetu column name that maps to the HBase row ID. Default is the first column.",
                        null,
                        false);
        PropertyMetadata<String> s6 =
                new PropertyMetadata<>(
                        SERIALIZER,
                        "Serializer for HBase data encodings. Can either be 'default', 'string'."
                                + "Default is 'default', i.e. the value from "
                                + "HBaseRowSerializer.getDefault(), i.e.",
                        VarcharType.VARCHAR,
                        String.class,
                        HBaseRowSerializer.getDefault().getClass().getName(),
                        false,
                        decoder -> decoder.equals("default")
                                ? HBaseRowSerializer.getDefault().getClass().getName()
                                : StringRowSerializer.class.getName(),
                        object -> object);
        PropertyMetadata<String> s7 =
                stringProperty(
                        HBASE_TABLE_NAME,
                        "Point to the table of hbase, establish the mapping of external access",
                        null,
                        false);
        PropertyMetadata<String> s8 =
                stringProperty(
                        SPLIT_BY_CHAR,
                        "Create multi-splits by the first char of rowKey",
                        "0~9,a~z,A~Z",
                        false);

        tableProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7, s8);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    /**
     * Gets the value of the column_mapping property, or Optional.empty() if not set.
     * <p>
     * Parses the value into a map of Hetu column name to a pair of strings, the HBase column family and qualifier.
     *
     * @param tableProperties The map of table properties
     * @return The column mapping, hetu name to (accumulo column family, qualifier)
     */
    public static Optional<Map<String, Pair<String, String>>> getColumnMapping(Map<String, Object> tableProperties)
    {
        Optional<String> strMapping = getStringFromProperties(COLUMN_MAPPING, tableProperties);
        if (!strMapping.isPresent()) {
            return Optional.empty();
        }

        // Parse out the column mapping list : "column_name:family:qualifier"
        ImmutableMap.Builder<String, Pair<String, String>> mapping = ImmutableMap.builder();
        for (String m1 : strMapping.get().split(COMMA_SPLITTER)) {
            String[] tokens = m1.trim().split(COLON_SPLITTER);
            checkState(
                    tokens.length == Constants.NUMBER3,
                    "Mapping of %s contains %s tokens instead of 3",
                    m1, tokens.length);
            mapping.put(tokens[0].trim(), Pair.of(tokens[1].trim(), tokens[Constants.NUMBER2].trim()));
        }

        return Optional.of(mapping.build());
    }

    /**
     * getIndexColumns
     *
     * @param tableProperties tableProperties
     * @return Optional
     */
    public static Optional<List<String>> getIndexColumns(Map<String, Object> tableProperties)
    {
        Optional<String> indexColumns = getStringFromProperties(INDEX_COLUMNS, tableProperties);
        if (!indexColumns.isPresent()) {
            return Optional.empty();
        }
        else {
            return Optional.of(Arrays.asList(StringUtils.split(indexColumns.get(), ',')));
        }
    }

    /**
     * getIndexColumns
     *
     * @param tableProperties tableProperties
     * @return Optional
     */
    public static Optional<String> getIndexColumnsAsStr(Map<String, Object> tableProperties)
    {
        return getStringFromProperties(INDEX_COLUMNS, tableProperties);
    }

    /**
     * Gets the configured locality groups.
     *
     * @param tableProperties tableProperties
     * @return Optional
     */
    public static Optional<Map<String, Set<String>>> getLocalityGroups(Map<String, Object> tableProperties)
    {
        Optional<String> groupStr = getStringFromProperties(LOCALITY_GROUPS, tableProperties);
        if (!groupStr.isPresent()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<String, Set<String>> groups = ImmutableMap.builder();

        // Split all configured locality groups
        for (String group : groupStr.get().split(PIPE_SPLITTER)) {
            String[] locGroups = group.trim().split(COLON_SPLITTER);
            if (locGroups.length != Constants.NUMBER2) {
                throw new PrestoException(
                        INVALID_TABLE_PROPERTY,
                        "Locality groups string is malformed. See documentation for proper format.");
            }

            String grpName = locGroups[0];
            ImmutableSet.Builder<String> colSet = ImmutableSet.builder();

            for (String f : locGroups[1].trim().split(COMMA_SPLITTER)) {
                colSet.add(f.trim().toLowerCase(Locale.ENGLISH));
            }

            groups.put(grpName.toLowerCase(Locale.ENGLISH), colSet.build());
        }

        return Optional.of(groups.build());
    }

    /**
     * getRowId
     *
     * @param tableProperties tableProperties
     * @return Optional
     */
    public static Optional<String> getRowId(Map<String, Object> tableProperties)
    {
        return getStringFromProperties(ROW_ID, tableProperties);
    }

    /**
     * Gets the {@link HBaseRowSerializer} class name to use for this table
     *
     * @param tableProperties The map of table properties
     * @return The name of the HBaseRowSerializer class
     */
    public static Optional<String> getSerializerClass(Map<String, Object> tableProperties)
    {
        return getStringFromProperties(SERIALIZER, tableProperties);
    }

    /**
     * isExternal
     *
     * @param tableProperties tableProperties
     * @return boolean
     */
    public static boolean isExternal(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        boolean isExternal = Boolean.parseBoolean(tableProperties.get(EXTERNAL).toString());
        return isExternal;
    }

    /**
     * getHBaseTableName
     *
     * @param tableProperties tableProperties
     * @return Optional<String>
     */
    public static Optional<String> getHBaseTableName(Map<String, Object> tableProperties)
    {
        return getStringFromProperties(HBASE_TABLE_NAME, tableProperties);
    }

    /**
     * getSplitByChar
     *
     * @param tableProperties tableProperties
     * @return Optional<String>
     */
    public static Optional<String> getSplitByChar(Map<String, Object> tableProperties)
    {
        return getStringFromProperties(SPLIT_BY_CHAR, tableProperties);
    }

    private static Optional<String> getStringFromProperties(String key, Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String value = String.valueOf(tableProperties.get(key));
        if (Constants.S_NULL.equals(value)) {
            return Optional.empty();
        }
        return Optional.ofNullable(value);
    }
}
