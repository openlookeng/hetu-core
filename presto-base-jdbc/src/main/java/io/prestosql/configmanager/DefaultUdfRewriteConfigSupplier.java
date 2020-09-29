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
package io.prestosql.configmanager;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DefaultUdfRewriteConfigSupplier
        implements ConfigSupplier
{
    private final Map<String, String> udfConfigMap;

    public DefaultUdfRewriteConfigSupplier(Map<String, String> udfConfigMap)
    {
        requireNonNull(udfConfigMap);
        this.udfConfigMap = new HashMap<>(udfConfigMap);
    }

    @Override
    public Map<String, String> getConfigKeyValueMap()
    {
        return ImmutableMap.copyOf(this.udfConfigMap);
    }

    @Override
    public Optional<String> getConfigValue(String key)
    {
        if (this.udfConfigMap.containsKey(key)) {
            return Optional.of(this.udfConfigMap.get(key));
        }
        else {
            return Optional.empty();
        }
    }
}
