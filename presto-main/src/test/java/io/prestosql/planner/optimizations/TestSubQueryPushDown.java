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
package io.prestosql.planner.optimizations;

import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.builder.optimizer.SubQueryPushDown;
import io.prestosql.utils.MockLocalQueryRunner;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ReturnsArgumentAt;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TestSubQueryPushDown
{
    @Test
    public void testSubQueryPushDownEnabled()
    {
        SubQueryPushDown optimizer = Mockito.mock(SubQueryPushDown.class);
        when(optimizer.optimize(any(), any(), any(), any(), any(), any())).then(new ReturnsArgumentAt(0));

        MockLocalQueryRunner queryRunner = new MockLocalQueryRunner(ImmutableMap.of("query_pushdown", "true"));
        queryRunner.init();
        queryRunner.createPlan("SELECT * FROM orders limit 10", optimizer);

        Mockito.verify(optimizer, times(1)).optimize(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject());
    }

    @Test
    public void testSubQueryPushDownDisabled()
    {
        SubQueryPushDown optimizer = Mockito.mock(SubQueryPushDown.class);
        when(optimizer.optimize(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        MockLocalQueryRunner queryRunner = new MockLocalQueryRunner(ImmutableMap.of("query_pushdown", "false"));
        queryRunner.init();
        queryRunner.createPlan("SELECT * FROM orders limit 10", optimizer);

        Mockito.verify(optimizer, times(0)).optimize(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject());
    }
}
