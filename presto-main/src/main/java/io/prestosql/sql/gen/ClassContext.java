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
package io.prestosql.sql.gen;

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.Scope;
import io.prestosql.spi.relation.SpecialForm;

public class ClassContext
{
    public static final int SPLIT_EXPRESSION_WEIGHT_THRESHOLD = 16;
    public static final int SPLIT_EXPRESSION_DEPTH_THRESHOLD = 32;

    interface FilterMethodGenerator
    {
        BytecodeBlock generateNewFilterMethod(String newFilterFuncName, RowExpressionCompiler rowExpressionCompiler,
                                              BytecodeGenerator generator, SpecialForm filter, Scope callerScope);
    }

    private final FilterMethodGenerator filterMethodGenerator;

    private int depth;
    private int funcSeq;
    private int weight;

    private Scope scope;
    private ClassDefinition classDefinition;

    public ClassContext(ClassDefinition classDefinition, Scope scope, FilterMethodGenerator filterMethodGenerator)
    {
        this.classDefinition = classDefinition;
        this.scope = scope;
        this.filterMethodGenerator = filterMethodGenerator;
    }

    public Scope getScope()
    {
        return scope;
    }

    public ClassDefinition getClassDefinition()
    {
        return classDefinition;
    }

    public FilterMethodGenerator getFilterMethodGenerator()
    {
        return filterMethodGenerator;
    }

    public int getFuncSeq()
    {
        return funcSeq;
    }

    public void incrementFuncSeq()
    {
        funcSeq++;
    }

    public int getDepth()
    {
        return depth;
    }

    public void incrementDepth()
    {
        this.depth++;
    }

    public void decrementDepth()
    {
        this.depth--;
    }

    public int getWeight()
    {
        return weight;
    }

    public void resetWeight()
    {
        this.weight = 1;
    }

    public void addWeight(int weight)
    {
        this.weight += weight;
    }
}
