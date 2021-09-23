/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin;

import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.sql.Types;
import java.util.Optional;

import static io.prestosql.spi.type.Decimals.MAX_PRECISION;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.CHAR;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIME;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;

public class KylinUtil
{
    private static final int MAX_SCALE = 12;

    private KylinUtil()
    {
    }

    public static Optional<Integer> prestoTypeToJdbcType(Type type)
    {
        switch (type.getTypeSignature().getBase()) {
            case VARCHAR:
                return Optional.of(Types.VARCHAR);
            case DECIMAL:
                return Optional.of(Types.DECIMAL);
            case DATE:
                return Optional.of(Types.DATE);
            case INTEGER:
                return Optional.of(Types.INTEGER);
            case BIGINT:
                return Optional.of(Types.BIGINT);
            case TINYINT:
                return Optional.of(Types.TINYINT);
            case SMALLINT:
                return Optional.of(Types.SMALLINT);
            case BOOLEAN:
                return Optional.of(Types.BOOLEAN);
            case DOUBLE:
                return Optional.of(Types.DOUBLE);
            case TIMESTAMP:
                return Optional.of(Types.TIMESTAMP);
            case TIME:
                return Optional.of(Types.TIME);
            case REAL:
                return Optional.of(Types.REAL);
            case CHAR:
                return Optional.of(Types.CHAR);
            case VARBINARY:
                return Optional.of(Types.VARBINARY);
            default:
                return Optional.empty();
        }
    }

    public static <T> T getSessionProperty(ConnectorSession session, String key, Class<T> typeClass, T defaultValue)
    {
        try {
            return session.getProperty(key, typeClass);
        }
        catch (Exception ex) {
            return defaultValue;
        }
    }

    public static JdbcTypeHandle getJdbcTypeHandle(String outColumn, Type prestoType, Optional<Integer> optType)
    {
        int precision = 0;
        int scale = 0;
        if (prestoType instanceof DecimalType) {
            DecimalType originalType = (DecimalType) prestoType;
            precision = originalType.getPrecision();
            scale = originalType.getScale();
            //TODO we need to find solution from the explain plan and handle precision and scale
            if (isReservedName(outColumn)) {
                precision = MAX_PRECISION - MAX_SCALE;
                scale = MAX_SCALE;
            }
        }
        else if (prestoType instanceof VarcharType) {
            VarcharType originalType = (VarcharType) prestoType;
            precision = originalType.getLength().orElse(VarcharType.MAX_LENGTH);
        }

        JdbcTypeHandle typeHandle = new JdbcTypeHandle(optType.get(),
                Optional.ofNullable(prestoType.getDisplayName()), precision, scale,
                Optional.empty());
        return typeHandle;
    }

    public static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), decimalType.getPrecision(),
                decimalType.getScale(), Optional.empty()));
    }

    private static boolean isReservedName(String str)
    {
        return "sum".equalsIgnoreCase(str)
                || "count".equalsIgnoreCase(str)
                || "min".equalsIgnoreCase(str)
                || "max".equalsIgnoreCase(str)
                || "avg".equalsIgnoreCase(str)
                || "year".equalsIgnoreCase(str)
                || "month".equalsIgnoreCase(str)
                || "day".equalsIgnoreCase(str);
    }
}
