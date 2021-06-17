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
package io.prestosql.operator;

import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;

public interface GroupBySort
        extends GroupBy
{
    static GroupBy createGroupBySort(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        if (hashTypes.size() == 1 && hashTypes.get(0).equals(BIGINT) && hashChannels.length == 1) {
            return new BigintGroupBySort(hashChannels[0], inputHashChannel.isPresent(), expectedSize, updateMemory);
        }
        return new MultiChannelGroupBySort(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler);
    }
}
