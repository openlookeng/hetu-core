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
package io.prestosql.server;

import io.airlift.node.NodeInfo;
import io.prestosql.client.NodeVersion;
import io.prestosql.client.ServerInfo;
import io.prestosql.metadata.NodeState;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GroupProvider;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/info")
public class ServerInfoResource
{
    private final NodeVersion version;
    private final String environment;
    private final boolean coordinator;
    private final NodeStateChangeHandler nodeStateChangeHandler;
    private final long startTime = System.nanoTime();
    private final AtomicBoolean startupComplete = new AtomicBoolean();
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;

    @Inject
    public ServerInfoResource(NodeVersion nodeVersion,
            NodeInfo nodeInfo,
            ServerConfig serverConfig,
            NodeStateChangeHandler nodeStateChangeHandler,
            AccessControl accessControl,
            GroupProvider groupProvider)
    {
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.coordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
        this.nodeStateChangeHandler = requireNonNull(nodeStateChangeHandler, "nodeStateChangeHandler");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public ServerInfo getInfo()
    {
        boolean starting = !startupComplete.get();
        return new ServerInfo(version, environment, coordinator, starting, Optional.of(nanosSince(startTime)));
    }

    /**
     * Update state for this node. Whether a <code>state</code> value is allowed depends on current node state:
     * - ACTIVE, ISOLATING, ISOLATED: allowed when current state is one of these 3
     * - SHUTTING_DOWN: always allowed
     * - INACTIVE: never allowed
     * <p>
     * Result of setting a new <code>state</code>:
     * - ACTIVE: restore the node back to normal operation
     * - ISOLATING: request to isolate the node, by not assigning new workload on it,
     * and waiting for existing workloads to finish, upon which state becomes ISOLATED
     * - ISOLATED: immediately isolate this node, by not assigning new workload;
     * if there are ongoing workloads, they are deemed unimportant and may not finish.
     * The "isolated" state is useful for, e.g. maintenance work on the node.
     * - SHUTTING_DOWN: request to shut down the node, by not assigning new workload on it,
     * and waiting for existing workloads to finish, upon which the Java process exits
     * <p>
     * Note that the caller (resource provider) is responsible for making sure minimum number of active nodes
     * are available after isolation or shutdown requests.
     *
     * @param state New node state (Required)
     * @throws WebApplicationException if specified <code>state</code> is not allowed
     */
    @PUT
    @Path("state")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response updateState(NodeState state, @Context HttpServletRequest servletRequest)
            throws WebApplicationException
    {
        try {
            requireNonNull(state, "state is null");
        }
        catch (Exception ex) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        try {
            HttpRequestSessionContext httpRequestSessionContext = new HttpRequestSessionContext(servletRequest, groupProvider);
            accessControl.checkCanAccessNodeInfo(httpRequestSessionContext.getIdentity());
        }
        catch (AccessDeniedException e) {
            throw new ForbiddenException("No permission");
        }

        try {
            nodeStateChangeHandler.doStateTransition(state);
            return Response.ok().build();
        }
        catch (IllegalStateException e) {
            throw new WebApplicationException(Response
                    .status(BAD_REQUEST)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(format("Invalid state transition to %s", state))
                    .build());
        }
    }

    @GET
    @Path("state")
    @Produces(APPLICATION_JSON)
    public NodeState getServerState()
    {
        return nodeStateChangeHandler.getState();
    }

    @GET
    @Path("coordinator")
    @Produces(TEXT_PLAIN)
    public Response getServerCoordinator()
    {
        if (coordinator) {
            return Response.ok().build();
        }
        // return 404 to allow load balancers to only send traffic to the coordinator
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    public void startupComplete()
    {
        checkState(startupComplete.compareAndSet(false, true), "Server startup already marked as complete");
    }

    public boolean isStartupComplete()
    {
        return startupComplete.get();
    }
}
