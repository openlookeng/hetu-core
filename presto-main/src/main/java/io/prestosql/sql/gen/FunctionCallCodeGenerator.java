/*
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

import io.airlift.bytecode.BytecodeNode;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.ArgumentType.VALUE_TYPE;

public class FunctionCallCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(FunctionHandle functionHandle, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        FunctionAndTypeManager functionAndTypeManager = context.getFunctionManager();

        BuiltInScalarFunctionImplementation function = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionHandle);

        List<BytecodeNode> argumentsBytecode = new ArrayList<>();
        for (int i = 0; i < arguments.size(); i++) {
            RowExpression argument = arguments.get(i);
            BuiltInScalarFunctionImplementation.ArgumentProperty argumentProperty = function.getArgumentProperty(i);
            if (argumentProperty.getArgumentType() == VALUE_TYPE) {
                argumentsBytecode.add(context.generate(argument));
            }
            else {
                argumentsBytecode.add(context.generate(argument, Optional.of(argumentProperty.getLambdaInterface())));
            }
        }

        return context.generateCall(functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName(), function, argumentsBytecode);
    }
}
