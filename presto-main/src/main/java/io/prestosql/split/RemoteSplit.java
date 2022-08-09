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
package io.prestosql.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.exchange.ExchangeSourceHandle;
import org.openjdk.jol.info.ClassLayout;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.spi.EstimateSizeUtil.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class RemoteSplit
        implements ConnectorSplit
{
    //TODO(SURYA): replace old constructor with new one and check how to get estimatedSizeOf information.
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RemoteSplit.class).instanceSize();

    private final URI location;
    private final String instanceId;
    private final ExchangeInput exchangeInput;

    @JsonCreator
    public RemoteSplit(@JsonProperty("location") URI location, @JsonProperty("instanceId") String instanceId, @JsonProperty("exchangeInput") ExchangeInput exchangeInput)
    {
        this.location = location;
        this.instanceId = instanceId;
        this.exchangeInput = exchangeInput;
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getInstanceId()
    {
        return instanceId;
    }

    @JsonProperty
    public ExchangeInput getExchangeInput()
    {
        return exchangeInput;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", location)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + exchangeInput.getRetainedSizeInBytes();
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = DirectExchangeInput.class, name = "direct"),
            @JsonSubTypes.Type(value = SpoolingExchangeInput.class, name = "spool")})
    public interface ExchangeInput
    {
        long getRetainedSizeInBytes();

        TaskId getTaskId();
    }

    public static class DirectExchangeInput
            implements ExchangeInput
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(DirectExchangeInput.class).instanceSize();

        private final TaskId taskId;
        private final String location;

        @JsonCreator
        public DirectExchangeInput(
                @JsonProperty("taskId") TaskId taskId,
                @JsonProperty("location") String location)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.location = requireNonNull(location, "location is null");
        }

        @JsonProperty
        public TaskId getTaskId()
        {
            return taskId;
        }

        @JsonProperty
        public String getLocation()
        {
            return location;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("taskId", taskId)
                    .add("location", location)
                    .toString();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + taskId.getRetainedSizeInBytes()
                    + estimatedSizeOf(location);
        }
    }

    public static class SpoolingExchangeInput
            implements ExchangeInput
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SpoolingExchangeInput.class).instanceSize();

        private final List<ExchangeSourceHandle> exchangeSourceHandles;

        @JsonCreator
        public SpoolingExchangeInput(@JsonProperty("exchangeSourceHandles") List<ExchangeSourceHandle> exchangeSourceHandles)
        {
            this.exchangeSourceHandles = ImmutableList.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
        }

        @JsonProperty
        public List<ExchangeSourceHandle> getExchangeSourceHandles()
        {
            return exchangeSourceHandles;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("exchangeSourceHandles", exchangeSourceHandles)
                    .toString();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + estimatedSizeOf(exchangeSourceHandles, ExchangeSourceHandle::getRetainedSizeInBytes);
        }

        @Override
        public TaskId getTaskId()
        {
            return null;
        }
    }
}
