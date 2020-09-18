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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

import static io.hetu.core.security.authentication.kerberos.KerberosConstant.KERBEROS_OID;
import static org.ietf.jgss.GSSCredential.INDEFINITE_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_AND_ACCEPT;

public class KerberosAuthenticator
{
    private static final ILogger LOGGER = Logger.getLogger(KerberosAuthenticator.class);

    private static final String KRB5_CONFIG_FILE = "java.security.krb5.conf";

    private static final String JAAS_CONFIG_FILE = "java.security.auth.login.config";

    private static final int DEFAULT_CREDENTIAL_REFRESH_TIME_SEC = 60;

    private LoginContext loginContext;

    private Subject subject;

    private Principal principal;

    private GSSManager gssManager;

    private GSSCredential currentCredential;

    private String loginContextName;

    private String servicePrincipalName;

    public KerberosAuthenticator()
    {
        gssManager = GSSManager.getInstance();

        loginContextName = KerberosConfig.getLoginContextName();
        if (loginContextName == null || loginContextName.isEmpty()) {
            throw new KerberosException("Enable hazelcast authentication, login context name should be set.");
        }

        servicePrincipalName = KerberosConfig.getServicePrincipalName();
        if (servicePrincipalName == null || servicePrincipalName.isEmpty()) {
            throw new KerberosException("Enable hazelcast authentication, service principal name should be set.");
        }

        String krb5ConfigFile = System.getProperty(KRB5_CONFIG_FILE);
        if (krb5ConfigFile == null || krb5ConfigFile.isEmpty()) {
            throw new KerberosException("Enable hazelcast authentication, krb5 config file should be set.");
        }

        String jaasConfigFile = System.getProperty(JAAS_CONFIG_FILE);
        if (jaasConfigFile == null || jaasConfigFile.isEmpty()) {
            throw new KerberosException("Enable hazelcast authentication, jaas config file should be set.");
        }
    }

    /**
     * Kerberos login using jaas
     *
     * @throws LoginException
     * @throws GSSException
     */
    public void login() throws LoginException, GSSException
    {
        loginContext = new LoginContext(loginContextName);
        loginContext.login();

        subject = loginContext.getSubject();
        if (subject.getPrincipals().isEmpty()) {
            throw new LoginException("Empty principals in login subject");
        }

        for (Principal principal : subject.getPrincipals()) {
            this.principal = principal;
            break;
        }

        final GSSName gssName = gssManager.createName(getPrincipalShortName(), GSSName.NT_USER_NAME);
        currentCredential = doAs(new PrivilegedExceptionAction<GSSCredential>() {
            @Override
            public GSSCredential run() throws Exception
            {
                return gssManager.createCredential(gssName, INDEFINITE_LIFETIME, new Oid[] {
                        new Oid(KERBEROS_OID)
                }, INITIATE_AND_ACCEPT);
            }
        });

        LOGGER.info("Hazelcast kerberos login success.");
    }

    /**
     * Get the full name of principal
     *
     * @return The full name of principal name
     */
    public String getPrincipalFullName()
    {
        if (principal == null) {
            return null;
        }

        return principal.getName();
    }

    /**
     * Get the short name of principal
     *
     * @return The short name of principal name
     */
    public String getPrincipalShortName()
    {
        if (principal == null) {
            return null;
        }

        return principal.getName().split("@")[0];
    }

    /**
     * Authenticate using the accepted token
     *
     * @param credentials Kerberos token credentials
     * @throws GSSException
     */
    public Principal doAuthenticateFilter(KerberosTokenCredentials credentials) throws GSSException
    {
        checkCredentialAndRelogin();

        GSSContext context = doAs(new PrivilegedExceptionAction<GSSContext>()
        {
            @Override
            public GSSContext run() throws Exception
            {
                return gssManager.createContext(currentCredential);
            }
        });

        try {
            byte[] tokenBytes = Base64.getDecoder().decode(credentials.getToken());
            context.acceptSecContext(tokenBytes, 0, tokenBytes.length);
            if (context.isEstablished()) {
                return new KerberosPrincipal(context.getSrcName().toString());
            }
        }
        finally {
            context.dispose();
        }

        return null;
    }

    /**
     * Generate service token
     *
     * @return The kerberos token credentials
     * @throws GSSException
     */
    public KerberosTokenCredentials generateServiceToken() throws GSSException
    {
        checkCredentialAndRelogin();

        GSSContext context = doAs(new PrivilegedExceptionAction<GSSContext>() {
            @Override
            public GSSContext run() throws Exception
            {
                return gssManager.createContext(gssManager.createName(servicePrincipalName, GSSName.NT_USER_NAME),
                        new Oid(KERBEROS_OID), currentCredential, INDEFINITE_LIFETIME);
            }
        });

        byte[] tokenBytes = new byte[0];
        tokenBytes = context.initSecContext(tokenBytes, 0, tokenBytes.length);

        return new KerberosTokenCredentials(Base64.getEncoder().encode(tokenBytes));
    }

    private void checkCredentialAndRelogin()
    {
        try {
            if (currentCredential.getRemainingLifetime() <= DEFAULT_CREDENTIAL_REFRESH_TIME_SEC) {
                LOGGER.info("Hazelcast kerberos relogin ...");
                login();
            }
        }
        catch (LoginException | GSSException e) {
            LOGGER.severe("Hazelcast kerberos relogin failed.", e);
        }
    }

    private <T> T doAs(PrivilegedExceptionAction<T> action)
    {
        try {
            return Subject.doAs(loginContext.getSubject(), action);
        }
        catch (PrivilegedActionException e) {
            LOGGER.severe("Failed to do as action.", e);
            throw new KerberosException(e.getMessage());
        }
    }
}
