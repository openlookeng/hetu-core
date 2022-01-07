/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.queryeditorui.resources;

import com.google.inject.Inject;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.queryeditorui.security.UiAuthenticator;
import io.prestosql.server.security.WebUIAuthenticator;
import io.prestosql.spi.eventlistener.AuditLogEvent;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;

@Path("/")
public class LoginResource
{
    private WebUIAuthenticator uiAuthenticator;

    private EventListenerManager eventListenerManager;

    @Inject
    public LoginResource(WebUIAuthenticator uiAuthenticator, EventListenerManager eventListenerManager)
    {
        this.uiAuthenticator = uiAuthenticator;
        this.eventListenerManager = eventListenerManager;
    }

    @POST
    @Path("/ui/api/login")
    public Response login(
            @FormParam("username") String username,
            @FormParam("password") String password,
            @FormParam("redirectPath") String redirectPath,
            @Context SecurityContext securityContext,
            @Context HttpServletRequest request)
    {
        username = emptyToNull(username);
        password = emptyToNull(password);
        redirectPath = emptyToNull(redirectPath);
        Optional<NewCookie> newCookie = uiAuthenticator.checkLoginCredentials(username, password, securityContext.isSecure());
        String userAgent = request.getHeader("User-Agent");
        AuditLogEvent enhancedEvent = new AuditLogEvent(username, request.getRemoteAddr(), "", "WebUi", "");
        if (newCookie.isPresent()) {
            enhancedEvent.setOperation("WebUi login!, UserAgent=\'" + userAgent);
            enhancedEvent.setLevel("INFO");
            eventListenerManager.eventEnhanced(enhancedEvent);
            return UiAuthenticator.redirectFromSuccessfulLoginResponse(redirectPath)
                    .cookie(newCookie.get()).build();
        }
        // authentication failed, redirect back to the login page
        enhancedEvent.setOperation("Attempt to login WebUi failed! UserAgent=\'" + userAgent);
        enhancedEvent.setLevel("WARN");
        eventListenerManager.eventEnhanced(enhancedEvent);
        return Response.seeOther(UiAuthenticator.LOGIN_FORM_URI).build();
    }

    @POST
    @Path("/ui/api/logout")
    public Response logout(@Context SecurityContext securityContext, @Context HttpServletRequest request)
    {
        String user = emptyToNull(request.getHeader(PRESTO_USER));
        if (user == null) {
            user = (String) request.getAttribute(PRESTO_USER);
        }
        String operation = "WebUi logout!, UserAgent=" + request.getHeader("User-Agent");
        eventListenerManager.eventEnhanced(new AuditLogEvent(user, request.getRemoteAddr(), operation, "WebUi", "INFO"));
        return Response.seeOther(UiAuthenticator.LOGIN_FORM_URI)
                .cookie(UiAuthenticator.getDeleteCookie(securityContext.isSecure()))
                .build();
    }
}
