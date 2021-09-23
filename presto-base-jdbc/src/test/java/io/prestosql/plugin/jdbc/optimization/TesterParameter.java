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
package io.prestosql.plugin.jdbc.optimization;

import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;

public class TesterParameter
{
    private static final TesterParameter testerParameter = new TesterParameter();

    private final Metadata metadata;
    private final RowExpressionService rowExpressionService;
    private final DeterminismEvaluator determinismEvaluator;
    private final StandardFunctionResolution functionResolution;

    public static TesterParameter getTesterParameter()
    {
        return testerParameter;
    }

    public Metadata getMetadata()
    {
        return this.metadata;
    }

    public RowExpressionService getRowExpressionService()
    {
        return this.rowExpressionService;
    }

    public DeterminismEvaluator getDeterminismEvaluator()
    {
        return this.determinismEvaluator;
    }

    public StandardFunctionResolution getFunctionResolution()
    {
        return this.functionResolution;
    }

    private TesterParameter()
    {
        this.metadata = TestBaseJdbcPushDownBase.metadata;
        this.rowExpressionService = new ConnectorRowExpressionService(new RowExpressionDomainTranslator(metadata), new RowExpressionDeterminismEvaluator(metadata));
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
    }
}
