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
package io.prestosql.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class IndexAccessControlRule
{
    private final Optional<Pattern> userRegex;
    private final Set<IndexPrivilege> privileges;

    @JsonCreator
    public IndexAccessControlRule(
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("privileges") Set<IndexPrivilege> privileges)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
    }

    public Optional<Set<IndexPrivilege>> match(String user)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true)) {
            return Optional.of(privileges);
        }
        return Optional.empty();
    }

    public enum IndexPrivilege
    {
        CREATE, DROP, RENAME, UPDATE, SHOW, ALL
    }
}
