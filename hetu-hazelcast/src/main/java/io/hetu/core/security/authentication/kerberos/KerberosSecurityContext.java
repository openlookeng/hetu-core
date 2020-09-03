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
package io.hetu.core.security.authentication.kerberos;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.security.StaticCredentialsFactory;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.SecureCallable;
import com.hazelcast.security.SecurityContext;
import org.ietf.jgss.GSSException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.security.AccessControlException;
import java.security.Permission;
import java.util.Set;
import java.util.concurrent.Callable;

public class KerberosSecurityContext
        implements SecurityContext
{
    private static final ILogger LOGGER = Logger.getLogger(KerberosSecurityContext.class);

    private KerberosAuthenticator kerberosAuthenticator;

    public KerberosSecurityContext()
    {
        kerberosAuthenticator = new KerberosAuthenticator();
        try {
            kerberosAuthenticator.login();
        }
        catch (LoginException | GSSException e) {
            LOGGER.severe("Hazelcast kerberos login failed", e);
            throw new RuntimeException("Hazelcast kerberos login failed");
        }
    }

    public KerberosAuthenticator getKerberosAuthenticator()
    {
        return kerberosAuthenticator;
    }

    @Override
    public LoginContext createMemberLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException
    {
        return null;
    }

    @Override
    public LoginContext createClientLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException
    {
        return null;
    }

    @Override
    public ICredentialsFactory getCredentialsFactory()
    {
        ICredentialsFactory credentialsFactory;
        try {
            credentialsFactory = new StaticCredentialsFactory(kerberosAuthenticator.generateServiceToken());
        }
        catch (GSSException e) {
            throw new SecurityException(String.format("Failed to create kerberos credentials factory: %s", e.getMessage()));
        }

        return credentialsFactory;
    }

    @Override
    public void checkPermission(Subject subject, Permission permission) throws AccessControlException {}

    @Override
    public void interceptBefore(Credentials credentials, String serviceName, String objectName,
                                String methodName, Object[] parameters) throws AccessControlException {}

    @Override
    public void interceptAfter(Credentials credentials, String serviceName, String objectName, String methodName) {}

    @Override
    public <V> SecureCallable<V> createSecureCallable(Subject subject, Callable<V> callable)
    {
        return null;
    }

    @Override
    public <V> SecureCallable<?> createSecureCallable(Subject subject, Runnable runnable)
    {
        return null;
    }

    @Override
    public void destroy() {}

    @Override
    public void refreshPermissions(Set<PermissionConfig> permissionConfigs) {}
}
