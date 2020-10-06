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
package io.prestosql.plugin.tpch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TpchTableHandle
        implements ConnectorTableHandle
{
    private final String tableName;
    private final double scaleFactor;
    private final TupleDomain<ColumnHandle> constraint;

    public TpchTableHandle(String tableName, double scaleFactor)
    {
        this(tableName, scaleFactor, TupleDomain.all());
    }

    @JsonCreator
    public TpchTableHandle(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("scaleFactor") double scaleFactor,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        checkArgument(scaleFactor > 0, "Scale factor must be larger than 0");
        this.scaleFactor = scaleFactor;
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String getSchemaPrefixedTableName()
    {
        if (this.scaleFactor == 0.01) {
            // See io.prestosql.plugin.tpch.TpchMetadata.schemaNameToScaleFactor
            return "tiny." + this.tableName;
        }
        return "sf" + ((int) this.scaleFactor) + "." + this.tableName;
    }

    @JsonProperty
    public double getScaleFactor()
    {
        return scaleFactor;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    /**
     * Presto execution plan caching functionality requires a method to update
     * {@link ConnectorTableHandle} from a previous execution plan with new info from
     * a new query. This is a workaround for presto-main module to modify connector table
     * handles in a generic way without needing access to classes in connector packages or
     * knowledge of connector specific constructor implementations. This method must be
     * overwritten in the connector for execution plan caching functionality to be supported
     *
     * @param oldConnectorTableHandle connector table handle containing information
     * to be passed into a {@link ConnectorTableHandle}
     * @return new {@link ConnectorTableHandle} containing connector specific properties
     * from the oldConnectorTableHandle
     */
    @Override
    public ConnectorTableHandle createFrom(ConnectorTableHandle oldConnectorTableHandle)
    {
        TpchTableHandle oldTpchTableHandle = (TpchTableHandle) oldConnectorTableHandle;
        return new TpchTableHandle(tableName, scaleFactor, oldTpchTableHandle.getConstraint());
    }

    @Override
    public String toString()
    {
        return tableName + ":sf" + scaleFactor;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, scaleFactor);
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
        TpchTableHandle other = (TpchTableHandle) obj;
        return Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.scaleFactor, other.scaleFactor);
    }
}
