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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.client.NodeVersion;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;

import java.net.URI;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

/**
 * A node is a server in a cluster than can process queries.
 */
public class InternalNode
        implements Node
{
    private final String nodeIdentifier;
    private final URI internalUri;
    private final NodeVersion nodeVersion;
    private final boolean coordinator;
    private final boolean worker;

    @JsonCreator
    public InternalNode(
            @JsonProperty("nodeIdentifier") String nodeIdentifier,
            @JsonProperty("internalUri") URI internalUri,
            @JsonProperty("nodeVersion") NodeVersion nodeVersion,
            @JsonProperty("coordinator") boolean coordinator)
    {
        String trimNodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(trimNodeIdentifier, "nodeIdentifier is null or empty");
        this.internalUri = requireNonNull(internalUri, "internalUri is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
        this.worker = !coordinator;
    }

    @JsonCreator
    public InternalNode(
            @JsonProperty("nodeIdentifier") String nodeIdentifier,
            @JsonProperty("internalUri") URI internalUri,
            @JsonProperty("nodeVersion") NodeVersion nodeVersion,
            @JsonProperty("coordinator") boolean coordinator,
            @JsonProperty("worker") boolean worker)
    {
        String trimNodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(trimNodeIdentifier, "nodeIdentifier is null or empty");
        this.internalUri = requireNonNull(internalUri, "internalUri is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
        this.worker = worker;
    }

    @Override
    @JsonProperty
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    @JsonProperty
    public String getHost()
    {
        return internalUri.getHost();
    }

    @Override
    @Deprecated
    public URI getHttpUri()
    {
        return getInternalUri();
    }

    @JsonProperty
    public URI getInternalUri()
    {
        return internalUri;
    }

    @Override
    @JsonProperty
    public HostAddress getHostAndPort()
    {
        return HostAddress.fromUri(internalUri);
    }

    @Override
    @JsonProperty
    public String getVersion()
    {
        return nodeVersion.getVersion();
    }

    @Override
    @JsonProperty
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Override
    @JsonProperty
    public boolean isWorker()
    {
        return worker;
    }

    @JsonProperty
    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        InternalNode o = (InternalNode) obj;
        return nodeIdentifier.equals(o.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return nodeIdentifier.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("internalUri", internalUri)
                .add("nodeVersion", nodeVersion)
                .toString();
    }
}
