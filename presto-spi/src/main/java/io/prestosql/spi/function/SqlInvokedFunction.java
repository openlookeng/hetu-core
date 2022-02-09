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
package io.prestosql.spi.function;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.type.TypeSignature;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.prestosql.spi.function.FunctionKind.EXTERNAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class SqlInvokedFunction
        implements SqlFunction
{
    private final List<Parameter> parameters;
    private final String description;
    private final RoutineCharacteristics routineCharacteristics;
    private final String body;

    private final Signature signature;
    private final SqlFunctionId functionId;
    private final Optional<SqlFunctionHandle> functionHandle;
    private final Map<String, String> functionProperties;

    public SqlInvokedFunction(
            QualifiedObjectName functionName,
            List<Parameter> parameters,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            Map<String, String> functionProperties,
            Optional<String> version)
    {
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.description = requireNonNull(description, "description is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.body = requireNonNull(body, "body is null");

        List<TypeSignature> argumentTypes = parameters.stream()
                .map(Parameter::getType)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
        this.signature = new Signature(functionName, EXTERNAL, returnType, argumentTypes);
        this.functionId = new SqlFunctionId(functionName, argumentTypes);
        requireNonNull(functionProperties, "functionProperties is null");
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : functionProperties.entrySet()) {
            builder.put(entry.getKey(), entry.getValue());
        }
        this.functionProperties = builder.build();
        this.functionHandle = version.map(v -> new SqlFunctionHandle(this.functionId, v));
    }

    public SqlInvokedFunction withVersion(String version)
    {
        if (getVersion().isPresent()) {
            throw new IllegalArgumentException(format("function %s is already with version %s", signature.getName(), getVersion().get()));
        }
        return new SqlInvokedFunction(
                signature.getName(),
                parameters,
                signature.getReturnType(),
                description,
                routineCharacteristics,
                body,
                functionProperties,
                Optional.of(version));
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return routineCharacteristics.isDeterministic();
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return routineCharacteristics.isCalledOnNullInput();
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    public List<Parameter> getParameters()
    {
        return parameters;
    }

    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public String getBody()
    {
        return body;
    }

    public Map<String, String> getFunctionProperties()
    {
        return functionProperties;
    }

    public SqlFunctionId getFunctionId()
    {
        return functionId;
    }

    public Optional<SqlFunctionHandle> getFunctionHandle()
    {
        return functionHandle;
    }

    public Optional<String> getVersion()
    {
        return functionHandle.map(SqlFunctionHandle::getVersion);
    }

    public SqlFunctionHandle getRequiredFunctionHandle()
    {
        Optional<? extends SqlFunctionHandle> sqlFunctionHandle = getFunctionHandle();
        if (!sqlFunctionHandle.isPresent()) {
            throw new IllegalStateException("missing functionHandle");
        }
        return sqlFunctionHandle.get();
    }

    public String getRequiredVersion()
    {
        Optional<String> version = getVersion();
        if (!version.isPresent()) {
            throw new IllegalStateException("missing version");
        }
        return version.get();
    }

    public boolean hasSameDefinitionAs(SqlInvokedFunction function)
    {
        if (function == null) {
            throw new IllegalArgumentException("function is null");
        }
        return Objects.equals(parameters, function.parameters)
                && Objects.equals(description, function.description)
                && Objects.equals(routineCharacteristics, function.routineCharacteristics)
                && Objects.equals(body, function.body)
                && Objects.equals(signature, function.signature);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlInvokedFunction o = (SqlInvokedFunction) obj;
        return Objects.equals(parameters, o.parameters)
                && Objects.equals(description, o.description)
                && Objects.equals(routineCharacteristics, o.routineCharacteristics)
                && Objects.equals(body, o.body)
                && Objects.equals(signature, o.signature)
                && Objects.equals(functionId, o.functionId)
                && Objects.equals(functionHandle, o.functionHandle)
                && Objects.equals(functionProperties, o.functionProperties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parameters, description, routineCharacteristics, body, signature, functionId, functionHandle, functionProperties);
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder("{");
        for (Map.Entry<String, String> entry : functionProperties.entrySet()) {
            str.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
        }
        str.append("}");

        return format(
                "%s(%s):%s%s {%s} %s %s",
                signature.getName(),
                parameters.stream()
                        .map(Object::toString)
                        .collect(joining(",")),
                signature.getReturnType(),
                getVersion().map(version -> ":" + version).orElse(""),
                body,
                routineCharacteristics,
                str.toString());
    }
}
