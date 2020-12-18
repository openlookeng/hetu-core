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
package io.prestosql.operator.window;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.FunctionKind.WINDOW;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public enum RankingFunction
{
    ROW_NUMBER(new Signature("row_number", WINDOW, parseTypeSignature(StandardTypes.BIGINT), ImmutableList.of())), RANK(new Signature("rank", WINDOW, parseTypeSignature(StandardTypes.BIGINT), ImmutableList.of())), DENSE_RANK(new Signature("dense_rank", WINDOW, parseTypeSignature(StandardTypes.BIGINT), ImmutableList.of()));
    private Signature value;

    private RankingFunction(Signature value)
    {
        this.value = value;
    }

    public Signature getValue()
    {
        return this.value;
    }
}
