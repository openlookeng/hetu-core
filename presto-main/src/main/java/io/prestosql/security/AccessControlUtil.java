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
package io.prestosql.security;

import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.server.ServerConfig;
import io.prestosql.server.SessionContext;
import io.prestosql.spi.security.Identity;

import javax.servlet.http.HttpServletRequest;

import java.util.Optional;

public class AccessControlUtil
{
    private AccessControlUtil()
    {
    }

    public static Optional<String> getUserForFilter(AccessControl accessControl,
                ServerConfig serverConfig,
                HttpServletRequest servletRequest)
    {
        String sessionUser = AccessControlUtil.getUser(accessControl, new HttpRequestSessionContext(servletRequest));
        Optional<String> user = Optional.of(sessionUser);
        // if the user is admin, don't filter results by user.
        if (serverConfig.isAdmin(sessionUser)) {
            user = Optional.empty();
        }
        return user;
    }

    public static String getUser(AccessControl accessControl, SessionContext sessionContext)
    {
        checkCanImpersonateUser(accessControl, sessionContext);
        return sessionContext.getIdentity().getUser();
    }

    public static void checkCanImpersonateUser(AccessControl accessControl, SessionContext sessionContext)
    {
        Identity identity = sessionContext.getIdentity();
        // authenticated identity is not present for HTTP or authentication is not setup
        sessionContext.getAuthenticatedIdentity().ifPresent(authenticatedIdentity -> {
            // only check impersonation is authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(identity.getUser())) {
                accessControl.checkCanImpersonateUser(authenticatedIdentity, identity.getUser());
            }
        });
    }
}
