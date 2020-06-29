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
import io.prestosql.sql.planner.optimizations.LimitPushDown;
import io.prestosql.utils.MockLocalQueryRunner;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TestLimitPushDown
{
    @Test
    public void testLimitPushDownEnabled()
    {
        LimitPushDown optimizer = Mockito.mock(LimitPushDown.class);
        when(optimizer.optimize(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        MockLocalQueryRunner queryRunner = new MockLocalQueryRunner(ImmutableMap.of("push_limit_down", "true"));
        queryRunner.init();
        queryRunner.createPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey limit 10", optimizer);

        Mockito.verify(optimizer, times(1)).optimize(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject());
    }

    @Test
    public void testLimitPushDownDisabled()
    {
        LimitPushDown optimizer = Mockito.mock(LimitPushDown.class);
        when(optimizer.optimize(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        MockLocalQueryRunner queryRunner = new MockLocalQueryRunner(ImmutableMap.of("push_limit_down", "false"));
        queryRunner.init();
        queryRunner.createPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey limit 10", optimizer);

        Mockito.verify(optimizer, times(0)).optimize(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject());
    }
}
