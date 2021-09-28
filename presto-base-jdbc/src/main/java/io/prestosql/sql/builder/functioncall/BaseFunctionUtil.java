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
package io.prestosql.sql.builder.functioncall;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.connector.CatalogSchemaName.DEFAULT_NAMESPACE;
import static io.prestosql.sql.builder.functioncall.FunctionCallConstants.CATALOG_SCHEMA_LENGTH_COUNT;
import static io.prestosql.sql.builder.functioncall.FunctionCallConstants.DOT_SPLITTER;
import static io.prestosql.sql.builder.functioncall.FunctionCallConstants.REMOTE_CATALOGSCHEMAS_CONFIG_SPLITTER;
import static java.util.stream.Collectors.joining;

/**
 * Base Function Util
 *
 * @since 2019-12-17
 */
public class BaseFunctionUtil
{
    private BaseFunctionUtil()
    {
    }

    /**
     * formate identifier
     *
     * @param qualifiedNames qualified names
     * @param identifier identifier
     * @return sql statement
     */
    public static String formatIdentifier(Optional<Map<String, Selection>> qualifiedNames, String identifier)
    {
        if (qualifiedNames.isPresent()) {
            return qualifiedNames.get().get(identifier).getExpression();
        }
        return identifier;
    }

    /**
     * formate qualified name
     *
     * @param name qualified name
     * @return sql statement
     */
    public static String formatQualifiedName(QualifiedName name)
    {
        return name.getParts()
                .stream()
                .map(identifier -> formatIdentifier(Optional.empty(), identifier))
                .collect(joining("."));
    }

    /**
     * to verify if a function call is default builtin function or not
     *
     * @param callExpression call
     * @return true indicate that call is default builtin, false for not
     */
    public static boolean isDefaultFunction(CallExpression callExpression)
    {
        CatalogSchemaName catalogSchemaName = callExpression.getFunctionHandle().getFunctionNamespace();
        return DEFAULT_NAMESPACE.equals(catalogSchemaName);
    }

    /**
     * parser the supported function namespace of this connector
     */
    public static List<CatalogSchemaName> parserPushDownSupportedRemoteCatalogSchema(String pushDownExternalFunctionNamespaceStr)
    {
        if (pushDownExternalFunctionNamespaceStr == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(pushDownExternalFunctionNamespaceStr.split(REMOTE_CATALOGSCHEMAS_CONFIG_SPLITTER)).map(str -> {
            String[] catalogSchema = str.trim().split(DOT_SPLITTER);
            if (catalogSchema.length != CATALOG_SCHEMA_LENGTH_COUNT) {
                throw new IllegalArgumentException("Wrong config value, must contain catalog.schema");
            }
            return new CatalogSchemaName(catalogSchema[0], catalogSchema[1]);
        }).collect(Collectors.toList());
    }

    /**
     * parser jdbc connector's registry external function namespace
     */
    public static Optional<CatalogSchemaName> parserExternalFunctionCatalogSchema(String connectorRegistryFunctionNamespace)
    {
        if (connectorRegistryFunctionNamespace == null) {
            return Optional.empty();
        }
        String[] catalogSchema = connectorRegistryFunctionNamespace.trim().split(DOT_SPLITTER);
        if (catalogSchema.length != CATALOG_SCHEMA_LENGTH_COUNT) {
            throw new IllegalArgumentException("Wrong config value, must contain catalog.schema");
        }
        return Optional.of(new CatalogSchemaName(catalogSchema[0], catalogSchema[1]));
    }
}
