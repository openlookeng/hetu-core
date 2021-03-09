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
package io.prestosql.plugin.tpcds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TpcdsTableHandle
        implements ConnectorTableHandle
{
    private final String tableName;
    private final double scaleFactor;

    @JsonCreator
    public TpcdsTableHandle(@JsonProperty("tableName") String tableName, @JsonProperty("scaleFactor") double scaleFactor)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        checkState(scaleFactor > 0, "scaleFactor is negative");
        this.scaleFactor = scaleFactor;
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
            // See io.prestosql.plugin.tpcds.TpcdsMetadata.schemaNameToScaleFactor
            return "tiny." + this.tableName;
        }
        return "sf" + ((int) this.scaleFactor) + "." + this.tableName;
    }

    @JsonProperty
    public double getScaleFactor()
    {
        return scaleFactor;
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
        return new TpcdsTableHandle(tableName, scaleFactor);
    }

    @Override
    public String toString()
    {
        return "tpcds:" + tableName + ":sf" + scaleFactor;
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
        TpcdsTableHandle other = (TpcdsTableHandle) obj;
        return Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.scaleFactor, other.scaleFactor);
    }
}
