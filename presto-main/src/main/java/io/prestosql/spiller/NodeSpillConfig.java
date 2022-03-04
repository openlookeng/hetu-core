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
package io.prestosql.spiller;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class NodeSpillConfig
{
    private DataSize maxSpillPerNode = new DataSize(100, DataSize.Unit.GIGABYTE);
    private DataSize queryMaxSpillPerNode = new DataSize(100, DataSize.Unit.GIGABYTE);

    private boolean spillCompressionEnabled;
    private boolean spillEncryptionEnabled;

    private boolean spillDirectSerdeEnabled;

    private int spillPrefetchReadPages = 1;
    private boolean spillUseKryoSerialization;

    @NotNull
    public DataSize getMaxSpillPerNode()
    {
        return maxSpillPerNode;
    }

    @Config("experimental.max-spill-per-node")
    public NodeSpillConfig setMaxSpillPerNode(DataSize maxSpillPerNode)
    {
        this.maxSpillPerNode = maxSpillPerNode;
        return this;
    }

    @NotNull
    public DataSize getQueryMaxSpillPerNode()
    {
        return queryMaxSpillPerNode;
    }

    @Config("experimental.query-max-spill-per-node")
    public NodeSpillConfig setQueryMaxSpillPerNode(DataSize queryMaxSpillPerNode)
    {
        this.queryMaxSpillPerNode = queryMaxSpillPerNode;
        return this;
    }

    public boolean isSpillCompressionEnabled()
    {
        return spillCompressionEnabled;
    }

    @Config("experimental.spill-compression-enabled")
    public NodeSpillConfig setSpillCompressionEnabled(boolean spillCompressionEnabled)
    {
        this.spillCompressionEnabled = spillCompressionEnabled;
        return this;
    }

    public boolean isSpillEncryptionEnabled()
    {
        return spillEncryptionEnabled;
    }

    @Config("experimental.spill-encryption-enabled")
    public NodeSpillConfig setSpillEncryptionEnabled(boolean spillEncryptionEnabled)
    {
        this.spillEncryptionEnabled = spillEncryptionEnabled;
        return this;
    }

    public boolean isSpillDirectSerdeEnabled()
    {
        return spillDirectSerdeEnabled;
    }

    @Config("experimental.spill-direct-serde-enabled")
    public NodeSpillConfig setSpillDirectSerdeEnabled(boolean spillDirectSerdeEnabled)
    {
        this.spillDirectSerdeEnabled = spillDirectSerdeEnabled;
        return this;
    }

    @Min(1)
    @Max(100)
    public int getSpillPrefetchReadPages()
    {
        return spillPrefetchReadPages;
    }

    @Config("experimental.spill-prefetch-read-pages")
    public NodeSpillConfig setSpillPrefetchReadPages(int spillPrefetchedReadPages)
    {
        this.spillPrefetchReadPages = spillPrefetchedReadPages;
        return this;
    }

    public boolean isSpillUseKryoSerialization()
    {
        return spillUseKryoSerialization;
    }

    @Config("experimental.spill-use-kryo-serialization")
    public NodeSpillConfig setSpillUseKryoSerialization(boolean spillUseKryoSerialization)
    {
        this.spillUseKryoSerialization = spillUseKryoSerialization;
        return this;
    }
}
