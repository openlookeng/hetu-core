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
package io.prestosql.spi.connector;

import io.prestosql.spi.predicate.Domain;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.function.Function;

public interface ConnectorTableHandle
{
    default boolean isFilterSupported()
    {
        return false;
    }

    /**
     * Hetu execution plan caching functionality requires a method to update
     * {@link ConnectorTableHandle} from a previous execution plan with new info from
     * a new query. This is a workaround for hetu-main module to modify connector table
     * handles in a generic way without needing access to classes in connector packages or
     * knowledge of connector specific constructor implementations. This method must be
     * overwritten in the connector for execution plan caching functionality to be supported
     *
     * @param oldConnectorTableHandle connector table handle containing information
     * to be passed into a {@link ConnectorTableHandle}
     * @return new {@link ConnectorTableHandle} containing connector specific properties
     * from the oldConnectorTableHandle
     */
    default ConnectorTableHandle createFrom(ConnectorTableHandle oldConnectorTableHandle)
    {
        throw new NotImplementedException();
    }

    /**
     * Hetu requires both schema and table name but not
     * all the connectors are providing the implementation.
     * <p>
     * Almost all the connectors have a `getTableName` method so that defining a
     * super method allows the {@link ConnectorTableHandle#getSchemaPrefixedTableName()} to
     * provide a default implementation by returning the table name.
     * </p>
     * <p>
     * The return type is usually String but TestingMetadata returns SchemaTableName.
     * Therefore, the returned type is defined as Object.
     * </p>
     *
     * @return the table name
     */
    default Object getTableName()
    {
        throw new NotImplementedException();
    }

    /**
     * io.prestosql.sql.planner.SqlQueryBuilder requires the qualified
     * name of the table to build the SQL query. Supporting ConnectorTableHandle
     * classes must override this method and return the qualified name with
     * schema name and table name.
     *
     * @return table name in 'schema.table' format if schema is available
     */
    default String getSchemaPrefixedTableName()
    {
        Object tableName = getTableName();
        if (tableName == null) {
            return null;
        }
        else {
            return tableName.toString();
        }
    }

    /* to check table is acid type */
    default boolean isDeleteAsInsertSupported()
    {
        return false;
    }

    default boolean isUpdateAsInsertSupported()
    {
        return false;
    }

    default boolean isSuitableForPushdown()
    {
        return false;
    }

    default boolean hasDisjunctFiltersPushdown()
    {
        return false;
    }

    default String getDisjunctFilterConditions(Function<Domain, String> printer)
    {
        return "";
    }

    /* This method simply checks if Table can be cached by the Connector */
    default boolean isTableCacheable()
    {
        return false;
    }

    /* This method checks if heuristic index can be created with the table format */
    default boolean isHeuristicIndexSupported()
    {
        return false;
    }

    /* This method checks if the predicate columns are partition columns */
    default boolean isPartitionColumn(String column)
    {
        return false;
    }

    /* This method checks if reuse table scan can be used*/
    default boolean isReuseTableScanSupported()
    {
        return false;
    }

    /* This method checks if table properties caching supported*/
    default boolean isTablePropertiesCacheSupported()
    {
        return false;
    }

    /* This method checks if Sort Based Aggregation can be used*/
    default boolean isSortBasedAggregationSupported()
    {
        return false;
    }

    /* this method validates for required ConnectorTableHandle parameters*/
    default boolean basicEquals(ConnectorTableHandle that)
    {
        return equals(that);
    }
}
