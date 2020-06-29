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

package io.hetu.core.heuristicindex.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveUtil;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * TableMetadata definition
 */
public class TableMetadata
{
    private static final Logger LOG = LoggerFactory.getLogger(TableMetadata.class);

    private final Set<HiveColumnHandle> regularColumns;
    private final Map<HiveColumnHandle, Type> partitionColumns;
    private final String uri;
    private final Table table;
    private final String outputFormat;

    /**
     * Construct the TableMetadata given the Table object
     *
     * @param table table to be extracted metadata with
     */
    public TableMetadata(Table table)
    {
        this.table = table;
        this.uri = table.getStorage().getLocation();
        this.outputFormat = table.getStorage().getStorageFormat().getOutputFormat();
        this.regularColumns = getRegularColumns(table);
        this.partitionColumns = getPartitionColumns(table);
    }

    public Set<HiveColumnHandle> getRegularColumns()
    {
        return regularColumns;
    }

    /**
     * Gets all the columns for the given table
     *
     * @param table target table
     * @return a set that contains all the columns for the table
     */
    public static Set<HiveColumnHandle> getRegularColumns(Table table)
    {
        return new HashSet<>(HiveUtil.getRegularColumnHandles(table));
    }

    /**
     * Gets the all the columns for the current table and their mappings to their Hetu column types
     *
     * @return An immutable map between each column and its Hetu type
     */
    public Map<HiveColumnHandle, Type> getSupportedColumnsWithTypeMapping()
    {
        ImmutableMap.Builder<HiveColumnHandle, Type> columns = ImmutableMap.builder();
        TypeRegistry typeRegistry = new TypeRegistry();

        for (HiveColumnHandle column : regularColumns) {
            try {
                Type type = typeRegistry.getType(new NullTypeManager(), column.getTypeSignature());
                if (type == null) {
                    continue;
                }
                columns.put(column, type);
            }
            catch (UncheckedExecutionException | ExecutionException e) {
                LOG.warn("Hive column type {} is not supported.", column.getTypeSignature());
            }
        }
        return columns.build();
    }

    /**
     * Gets the partition columns for this table
     *
     * @return Immutable Map containing the partition columns' {@link HiveColumnHandle} and its type
     */
    public Map<HiveColumnHandle, Type> getPartitionColumns()
    {
        return partitionColumns;
    }

    private static Map<HiveColumnHandle, Type> getPartitionColumns(Table table)
    {
        ImmutableMap.Builder<HiveColumnHandle, Type> columns = ImmutableMap.builder();
        TypeRegistry typeRegistry = new TypeRegistry();

        for (HiveColumnHandle column : HiveUtil.getPartitionKeyColumnHandles(table)) {
            try {
                Type type = typeRegistry.getType(new NullTypeManager(), column.getTypeSignature());
                columns.put(column, type);
            }
            catch (UncheckedExecutionException | ExecutionException e) {
                // log a warning but continue to next column
                LOG.warn("Hive column type {} is not supported.", column.getTypeSignature());
            }
        }

        return columns.build();
    }

    public String getUri()
    {
        return uri;
    }

    public Table getTable()
    {
        return table;
    }

    public String getOutputFormat()
    {
        return outputFormat;
    }

    /**
     * TypeManager that only throws UnsupportedOperationException
     *
     * @since 2020-02-18
     */
    private static class NullTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<ParametricType> getParametricTypes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            throw new UnsupportedOperationException();
        }
    }
}
