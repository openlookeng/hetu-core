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

import com.google.inject.Inject;
import io.prestosql.server.security.WebUIAuthenticator;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.Set;

@Path("/")
public class FileUserAuthenticatorResource
{
    private WebUIAuthenticator uiAuthenticator;
    private final ServerConfig serverConfig;

    @Inject
    public FileUserAuthenticatorResource(WebUIAuthenticator uiAuthenticator, ServerConfig serverConfig)
    {
        this.uiAuthenticator = uiAuthenticator;
        this.serverConfig = serverConfig;
    }

    @GET
    @Path("/authenticator/api/getAdminUsers")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getAdminUsers(@Context HttpServletRequest servletRequest)
    {
        Optional<String> authenticatedUsername = uiAuthenticator.getAuthenticatedUsername(servletRequest);
        Set<String> admins = serverConfig.getAdmins();
        if (!authenticatedUsername.isPresent()) {
            return Response.status(Response.Status.UNAUTHORIZED.getStatusCode(), "Please login").build();
        }
        if (!admins.contains(authenticatedUsername.get())) {
            return Response.status(Response.Status.UNAUTHORIZED.getStatusCode(), "Please use admin to login").build();
        }
        return admins;
    }
}
