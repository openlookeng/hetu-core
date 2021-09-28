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
package io.prestosql.spi.function;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExternalFunctionInfo
{
    protected Optional<String> functionName;
    protected Optional<String> description;
    protected List<String> inputArgs;
    protected Optional<String> returnType;
    protected boolean isHidden;
    protected boolean deterministic;
    protected boolean calledOnNullInput;

    public ExternalFunctionInfo(Builder builder)
    {
        this.functionName = builder.functionName;
        this.description = builder.description;
        this.inputArgs = builder.inputArgs;
        this.returnType = builder.returnType;
        this.isHidden = builder.isHidden;
        this.deterministic = builder.deterministic;
        this.calledOnNullInput = builder.calledOnNullInput;
    }

    public Optional<String> getFunctionName()
    {
        return functionName;
    }

    public Optional<String> getDescription()
    {
        return description;
    }

    public List<String> getInputArgs()
    {
        return inputArgs;
    }

    public Optional<String> getReturnType()
    {
        return returnType;
    }

    public boolean isHidden()
    {
        return isHidden;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        protected Optional<String> functionName;
        protected Optional<String> description;
        protected List<String> inputArgs;
        protected Optional<String> returnType;
        protected boolean isHidden;
        protected boolean deterministic = true;
        protected boolean calledOnNullInput;

        public Builder()
        {
            functionName = Optional.empty();
            description = Optional.empty();
            inputArgs = Collections.emptyList();
            returnType = Optional.empty();
        }

        public Builder functionName(String name)
        {
            requireNonNull(name);
            functionName = Optional.of(name);
            return this;
        }

        public Builder description(String desc)
        {
            requireNonNull(desc);
            description = Optional.of(desc);
            return this;
        }

        public Builder inputArgs(String... types)
        {
            requireNonNull(types);
            inputArgs = ImmutableList.copyOf(types);
            return this;
        }

        public Builder returnType(String type)
        {
            requireNonNull(type);
            returnType = Optional.of(type);
            return this;
        }

        public Builder isHidden(boolean isHidden)
        {
            this.isHidden = isHidden;
            return this;
        }

        public Builder deterministic(boolean deterministic)
        {
            this.deterministic = deterministic;
            return this;
        }

        public Builder calledOnNullInput(boolean calledOnNullInput)
        {
            this.calledOnNullInput = calledOnNullInput;
            return this;
        }

        public ExternalFunctionInfo build()
        {
            return new ExternalFunctionInfo(this);
        }
    }
}
