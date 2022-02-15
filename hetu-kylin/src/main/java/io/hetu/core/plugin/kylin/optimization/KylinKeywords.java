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
package io.hetu.core.plugin.kylin.optimization;

import com.google.common.collect.ImmutableSet;
import io.hetu.core.plugin.kylin.KylinConstants;

import java.util.Locale;
import java.util.Set;

public final class KylinKeywords
{
    private KylinKeywords() {}

    public static Set<String> getKeywords()
    {
        return ImmutableSet.<String>builder()
                .add("sum")
                .add("count")
                .add("min")
                .add("max")
                .add("rank")
                .build();
    }

    public static String getAlias(String name)
    {
        if (getKeywords().contains(name.toLowerCase(Locale.ENGLISH))) {
            return KylinConstants.KYLIN_IDENTIFIER_QUOTE + name.toUpperCase(Locale.ROOT) + KylinConstants.KYLIN_IDENTIFIER_QUOTE;
        }
        return name;
    }
}
