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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class WriteIdInfo
{
    private final long minWriteId;
    private final long maxWriteId;
    private final int statementId;

    @JsonCreator
    public WriteIdInfo(
            @JsonProperty("minWriteId") long minWriteId,
            @JsonProperty("maxWriteId") long maxWriteId,
            @JsonProperty("statementId") int statementId)
    {
        this.minWriteId = minWriteId;
        this.maxWriteId = maxWriteId;
        this.statementId = statementId;
    }

    @JsonProperty
    public long getMinWriteId()
    {
        return minWriteId;
    }

    @JsonProperty
    public long getMaxWriteId()
    {
        return maxWriteId;
    }

    @JsonProperty
    public int getStatementId()
    {
        return statementId;
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

        WriteIdInfo that = (WriteIdInfo) o;
        return minWriteId == that.minWriteId &&
                maxWriteId == that.maxWriteId &&
                statementId == that.statementId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minWriteId, maxWriteId, statementId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("minWriteId", minWriteId)
                .add("maxWriteId", maxWriteId)
                .add("statementId", statementId)
                .toString();
    }
}
