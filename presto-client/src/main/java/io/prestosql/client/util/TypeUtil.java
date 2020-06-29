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
package io.prestosql.client.util;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.prestosql.spi.type.ArrayParametricType.ARRAY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharParametricType.CHAR;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalParametricType.DECIMAL;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.MapParametricType.MAP;
import static io.prestosql.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.RowParametricType.ROW;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.UnknownType.UNKNOWN;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharParametricType.VARCHAR;

public class TypeUtil
{
    private static final Pattern DESCRIBE_TYPE_PATTERN = Pattern.compile("(?<type>[a-zA-Z1-9 _]+)");

    private static final Map<String, ParametricType> PARAMETRIC_TYPE_MAP
            = new ImmutableMap.Builder<String, ParametricType>().put(DECIMAL.getName(), DECIMAL)
            .put(CHAR.getName(), CHAR)
            .put(VARCHAR.getName(), VARCHAR)
            .put(ARRAY.getName(), ARRAY)
            .put(MAP.getName(), MAP)
            .put(ROW.getName(), ROW)
            .build();

    private TypeUtil()
    {
    }

    private static Type typeMapping(String type)
    {
        switch (type) {
            case StandardTypes.BIGINT:
                return BIGINT;
            case StandardTypes.INTEGER:
                return INTEGER;
            case StandardTypes.SMALLINT:
                return SMALLINT;
            case StandardTypes.TINYINT:
                return TINYINT;
            case StandardTypes.BOOLEAN:
                return BOOLEAN;
            case StandardTypes.DATE:
                return DATE;
            case StandardTypes.REAL:
                return REAL;
            case StandardTypes.DOUBLE:
                return DOUBLE;
            case StandardTypes.HYPER_LOG_LOG:
                return HYPER_LOG_LOG;
            case StandardTypes.P4_HYPER_LOG_LOG:
                return P4_HYPER_LOG_LOG;
            case StandardTypes.TIMESTAMP:
                return TIMESTAMP;
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP_WITH_TIME_ZONE;
            case StandardTypes.TIME:
                return TIME;
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return TIME_WITH_TIME_ZONE;
            case StandardTypes.VARBINARY:
                return VARBINARY;
            case "unknown":
                return UNKNOWN;
            default:
                return null;
        }
    }

    private static Type parametricType(TypeManager typeManager, TypeSignature typeSignature)
    {
        String typeName = typeSignature.getBase().toLowerCase(Locale.ENGLISH);
        ParametricType parametricType = PARAMETRIC_TYPE_MAP.get(typeName);
        if (parametricType != null) {
            List<TypeParameter> parameters = new ArrayList<>();
            for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
                TypeParameter typeParameter = TypeParameter.of(parameter, typeManager);
                parameters.add(typeParameter);
            }
            return parametricType.createType(typeManager, parameters);
        }
        return null;
    }

    /**
     * Parse type type.
     *
     * @param typeManager the type manager.
     * @param typeName the type name
     * @return the type
     */
    public static Type parseType(TypeManager typeManager, String typeName)
    {
        Type hetuType = null;

        Matcher matcher = DESCRIBE_TYPE_PATTERN.matcher(typeName);
        if (matcher.matches()) {
            String type = matcher.group("type");
            hetuType = typeMapping(type);
        }

        if (hetuType == null) {
            TypeSignature typeSignature = TypeSignature.parseTypeSignature(typeName);
            hetuType = parametricType(typeManager, typeSignature);
        }

        if (hetuType == null) {
            throw new IllegalArgumentException("Type " + typeName + " is not supported");
        }

        return hetuType;
    }
}
