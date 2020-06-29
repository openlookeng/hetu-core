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
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.prestosql.utils.MockLocalQueryRunner;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.plan.Patterns.limit;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TestPushLimitThroughJoin
{
    @Test
    public void testPushLimitThroughJoinEnabled()
    {
        PushLimitThroughOuterJoin rule = Mockito.mock(PushLimitThroughOuterJoin.class);
        when(rule.getPattern()).thenReturn(limit());
        when(rule.apply(any(), any(), any())).thenReturn(Rule.Result.empty());
        when(rule.isEnabled(any())).thenCallRealMethod();

        MockLocalQueryRunner queryRunner = new MockLocalQueryRunner(ImmutableMap.of("push_limit_through_outer_join", "true"));
        queryRunner.init();
        queryRunner.createPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey limit 10", rule);

        Mockito.verify(rule, times(1)).apply(anyObject(), anyObject(), anyObject());
    }

    @Test
    public void testPushLimitThroughJoinDisabled()
    {
        PushLimitThroughOuterJoin rule = Mockito.mock(PushLimitThroughOuterJoin.class);
        when(rule.getPattern()).thenReturn(limit());
        when(rule.apply(any(), any(), any())).thenReturn(Rule.Result.empty());
        when(rule.isEnabled(any())).thenCallRealMethod();

        MockLocalQueryRunner queryRunner = new MockLocalQueryRunner(ImmutableMap.of("push_limit_through_outer_join", "false"));
        queryRunner.init();
        queryRunner.createPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey limit 10", rule);

        Mockito.verify(rule, times(0)).apply(anyObject(), anyObject(), anyObject());
    }
}
