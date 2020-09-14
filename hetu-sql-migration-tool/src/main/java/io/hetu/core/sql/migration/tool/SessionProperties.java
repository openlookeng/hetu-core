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
package io.hetu.core.sql.migration.tool;

import io.hetu.core.sql.migration.SqlSyntaxType;
import io.prestosql.sql.parser.ParsingOptions;

import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;

public class SessionProperties
{
    private SqlSyntaxType sourceType;
    private ParsingOptions parsingOptions;
    private MigrationConfig migrationConfig;
    private boolean isConsolePrintEnable;
    private boolean isDebugEnable;

    public SqlSyntaxType getSourceType()
    {
        return sourceType;
    }

    public void setSourceType(SqlSyntaxType sourceType)
    {
        this.sourceType = sourceType;
    }

    public ParsingOptions getParsingOptions()
    {
        if (parsingOptions == null) {
            parsingOptions = new ParsingOptions(migrationConfig.isConvertDecimalAsDouble() ? AS_DOUBLE : AS_DECIMAL);
            return parsingOptions;
        }

        return parsingOptions;
    }

    public void setParsingOptions(boolean isConvertDecimalAsDouble)
    {
        parsingOptions = new ParsingOptions(isConvertDecimalAsDouble ? AS_DOUBLE : AS_DECIMAL);
    }

    public boolean isConsolePrintEnable()
    {
        return isConsolePrintEnable;
    }

    public void setConsolePrintEnable(boolean consolePrintEnable)
    {
        isConsolePrintEnable = consolePrintEnable;
    }

    public MigrationConfig getMigrationConfig()
    {
        return migrationConfig;
    }

    public void setMigrationConfig(MigrationConfig migrationConfig)
    {
        this.migrationConfig = migrationConfig;
    }

    public boolean isDebugEnable()
    {
        return isDebugEnable;
    }

    public void setDebugEnable(boolean debugEnable)
    {
        isDebugEnable = debugEnable;
    }
}
