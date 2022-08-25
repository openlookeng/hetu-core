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

import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Property;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.sql.analyzer.PropertyUtil.evaluateProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableProceduresPropertyManager
{
    private TableProceduresPropertyManager()
    {
    }

    public static TableProceduresPropertyManager getInstance()
    {
        return SingletonManager.INSTANCE;
    }

    private static class SingletonManager
    {
        private static final TableProceduresPropertyManager INSTANCE = new TableProceduresPropertyManager();
    }

    private final ConcurrentMap<Key, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();

    public void addProperties(CatalogName catalogName, String procedureName, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(procedureName, "procedureName is null");
        requireNonNull(catalogName, "catalogName is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        Key propertiesKey = new Key(catalogName, procedureName);
        checkState(connectorProperties.putIfAbsent(propertiesKey, propertiesByName) == null, "Properties for key %s are already registered", propertiesKey);
    }

    public void removeProperties(CatalogName catalogName)
    {
        Set<Key> keysToRemove = connectorProperties.keySet().stream()
                .filter(key -> catalogName.equals(key.getCatalogName()))
                .collect(toImmutableSet());
        for (Key key : keysToRemove) {
            connectorProperties.remove(key);
        }
    }

    public Map<String, Object> getProperties(
            Metadata metadata,
            CatalogName catalog,
            String procedureName,
            Map<String, Expression> sqlPropertyValues,
            Session session,
            AccessControl accessControl,
            List<Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = connectorProperties.get(new Key(catalog, procedureName));
        if (supportedProperties == null) {
            throw new PrestoException(NOT_FOUND, format("Catalog '%s' table procedure '%s' property not found", catalog, procedureName));
        }

        Map<String, Optional<Object>> propertyValues = evaluateProperties(
                metadata,
                sqlPropertyValues.entrySet().stream()
                        .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                        .collect(toImmutableList()),
                session,
                accessControl,
                parameters,
                true,
                supportedProperties,
                INVALID_PROCEDURE_ARGUMENT,
                format("catalog '%s' table procedure '%s' property", catalog, procedureName));
        return propertyValues.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow(() -> new NoSuchElementException("No value present"))));
    }

    static final class Key
    {
        private final CatalogName catalogName;
        private final String procedureName;

        private Key(CatalogName catalogName, String procedureName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.procedureName = requireNonNull(procedureName, "procedureName is null");
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public String getProcedureName()
        {
            return procedureName;
        }

        @Override
        public String toString()
        {
            return catalogName + ":" + procedureName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(catalogName, key.catalogName)
                    && Objects.equals(procedureName, key.procedureName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalogName, procedureName);
        }
    }
}
