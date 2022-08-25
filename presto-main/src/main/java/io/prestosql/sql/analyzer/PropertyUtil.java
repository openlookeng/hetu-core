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
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ParameterRewriter;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.Property;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class PropertyUtil
{
    private PropertyUtil() {}

    public static Map<String, Optional<Object>> evaluateProperties(
            Iterable<Property> setProperties,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties,
            Map<String, PropertyMetadata<?>> map,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Map<String, Optional<Object>> propertyValues = new LinkedHashMap<>();
        List<Expression> expressions = parameters.values().stream().collect(Collectors.toList());
        // Fill in user-specified properties
        for (Property property : setProperties) {
            // property names are case-insensitive and normalized to lower case
            String propertyName = property.getName().getValue().toLowerCase(ENGLISH);
            PropertyMetadata<?> propertyMetadata = map.get(propertyName);
            if (propertyMetadata == null) {
                throw new PrestoException(errorCode, format("%s '%s' does not exist", capitalize(propertyTypeDescription), propertyName));
            }

            Optional<Object> value;
            if (property.isSetToDefault()) {
                value = Optional.ofNullable(propertyMetadata.getDefaultValue());
            }
            else {
                value = Optional.of(evaluateProperty(
                        metadata,
                        property.getNonDefaultValue(),
                        propertyMetadata,
                        session,
                        accessControl,
                        expressions,
                        errorCode,
                        propertyTypeDescription));
            }

            propertyValues.put(propertyMetadata.getName(), value);
        }

        if (includeAllProperties) {
            for (PropertyMetadata<?> propertyMetadata : map.values()) {
                if (!propertyValues.containsKey(propertyMetadata.getName())) {
                    propertyValues.put(propertyMetadata.getName(), Optional.ofNullable(propertyMetadata.getDefaultValue()));
                }
            }
        }
        return ImmutableMap.copyOf(propertyValues);
    }

    public static Map<String, Optional<Object>> evaluateProperties(
            Metadata metadata,
            Iterable<Property> setProperties,
            Session session,
            AccessControl accessControl,
            List<Expression> parameters,
            boolean includeAllProperties,
            Map<String, PropertyMetadata<?>> map,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Map<String, Optional<Object>> propertyValues = new LinkedHashMap<>();

        // Fill in user-specified properties
        for (Property property : setProperties) {
            // property names are case-insensitive and normalized to lower case
            String propertyName = property.getName().getValue().toLowerCase(ENGLISH);
            PropertyMetadata<?> propertyMetadata = map.get(propertyName);
            if (propertyMetadata == null) {
                throw new PrestoException(errorCode, format("%s '%s' does not exist", capitalize(propertyTypeDescription), propertyName));
            }

            Optional<Object> value;
            if (property.isSetToDefault()) {
                value = Optional.ofNullable(propertyMetadata.getDefaultValue());
            }
            else {
                value = Optional.of(evaluateProperty(
                        metadata,
                        property.getNonDefaultValue(),
                        propertyMetadata,
                        session,
                        accessControl,
                        parameters,
                        errorCode,
                        propertyTypeDescription));
            }

            propertyValues.put(propertyMetadata.getName(), value);
        }

        if (includeAllProperties) {
            for (PropertyMetadata<?> propertyMetadata : map.values()) {
                if (!propertyValues.containsKey(propertyMetadata.getName())) {
                    propertyValues.put(propertyMetadata.getName(), Optional.ofNullable(propertyMetadata.getDefaultValue()));
                }
            }
        }
        return ImmutableMap.copyOf(propertyValues);
    }

    private static Object evaluateProperty(
            Metadata metadata,
            Expression expression,
            PropertyMetadata<?> property,
            Session session,
            AccessControl accessControl,
            List<Expression> parameters,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Object sqlObjectValue;
        try {
            Type expectedType = property.getSqlType();
            Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
            Object value = evaluateConstantExpression(metadata, rewritten, expectedType, session, accessControl, parameters);

            // convert to object value type of SQL type
            BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
            writeNativeValue(expectedType, blockBuilder, value);
            sqlObjectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);
        }
        catch (PrestoException e) {
            throw new PrestoException(
                    errorCode,
                    format(
                            "Invalid value for %s '%s': Cannot convert [%s] to %s",
                            propertyTypeDescription,
                            property.getName(),
                            expression,
                            property.getSqlType()),
                    e);
        }

        if (sqlObjectValue == null) {
            throw new PrestoException(
                    errorCode,
                    format(
                            "Invalid null value for %s '%s' from [%s]",
                            propertyTypeDescription,
                            property.getName(),
                            expression));
        }

        try {
            return property.decode(sqlObjectValue);
        }
        catch (Exception e) {
            throw new PrestoException(
                    errorCode,
                    format(
                            "Unable to set %s '%s' to [%s]: %s",
                            propertyTypeDescription,
                            property.getName(),
                            expression,
                            e.getMessage()),
                    e);
        }
    }

    private static String capitalize(String value)
    {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }
}
