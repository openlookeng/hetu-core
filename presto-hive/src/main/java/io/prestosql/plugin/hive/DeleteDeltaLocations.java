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
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Stores information about ACID DELETE_DELTA for a Partition
 */
public class DeleteDeltaLocations
{
    private final String partitionLocation;
    private final List<WriteIdInfo> deleteDeltas;

    @JsonCreator
    public DeleteDeltaLocations(
            @JsonProperty("partitionLocation") String partitionLocation,
            @JsonProperty("deleteDeltas") List<WriteIdInfo> deleteDeltas)
    {
        this.partitionLocation = requireNonNull(partitionLocation, "partitionLocation is null");
        this.deleteDeltas = ImmutableList.copyOf(requireNonNull(deleteDeltas, "deleteDeltas is null"));
        checkArgument(!deleteDeltas.isEmpty(), "deleteDeltas is empty");
    }

    @JsonProperty
    public String getPartitionLocation()
    {
        return partitionLocation;
    }

    @JsonProperty
    public List<WriteIdInfo> getDeleteDeltas()
    {
        return deleteDeltas;
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

        DeleteDeltaLocations that = (DeleteDeltaLocations) o;
        return partitionLocation.equals(that.partitionLocation) &&
                deleteDeltas.equals(that.deleteDeltas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionLocation, deleteDeltas);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionLocation", partitionLocation)
                .add("deleteDeltas", deleteDeltas)
                .toString();
    }

    public static Builder builder(Path partitionPath)
    {
        return new Builder(partitionPath);
    }

    public static class Builder
    {
        private final Path partitionLocation;
        private final ImmutableList.Builder<WriteIdInfo> deleteDeltaInfoBuilder = ImmutableList.builder();

        private Builder(Path partitionPath)
        {
            partitionLocation = requireNonNull(partitionPath, "partitionPath is null");
        }

        public Builder addDeleteDelta(Path deleteDeltaPath, long minWriteId, long maxWriteId, int statementId)
        {
            requireNonNull(deleteDeltaPath, "deleteDeltaPath is null");
            Path partitionPathFromDeleteDelta = deleteDeltaPath.getParent();
            checkArgument(
                    partitionLocation.equals(partitionPathFromDeleteDelta),
                    "Partition location in DeleteDelta '%s' does not match stored location '%s'",
                    deleteDeltaPath.getParent().toString(),
                    partitionLocation);

            deleteDeltaInfoBuilder.add(new WriteIdInfo(minWriteId, maxWriteId, statementId));
            return this;
        }

        public Optional<DeleteDeltaLocations> build()
        {
            List<WriteIdInfo> localDeleteDeltas = deleteDeltaInfoBuilder.build();
            if (localDeleteDeltas.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new DeleteDeltaLocations(partitionLocation.toString(), localDeleteDeltas));
        }
    }
}
