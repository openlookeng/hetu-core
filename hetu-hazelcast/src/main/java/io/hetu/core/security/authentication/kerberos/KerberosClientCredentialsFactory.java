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
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import org.ietf.jgss.GSSException;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

public class KerberosClientCredentialsFactory
        implements ICredentialsFactory
{
    private static final ILogger LOGGER = Logger.getLogger(KerberosClientCredentialsFactory.class);

    private KerberosAuthenticator kerberosAuthenticator;

    public KerberosClientCredentialsFactory()
    {
        kerberosAuthenticator = new KerberosAuthenticator();
        try {
            kerberosAuthenticator.login();
        }
        catch (LoginException | GSSException e) {
            LOGGER.severe("Hazelcast client kerberos login failed", e);
            throw new RuntimeException("Hazelcast client kerberos login failed");
        }
    }

    @Override
    public void configure(CallbackHandler callbackHandler) {}

    @Override
    public Credentials newCredentials()
    {
        Credentials credentials = null;
        try {
            credentials = kerberosAuthenticator.generateServiceToken();
        }
        catch (GSSException e) {
            LOGGER.severe("Hazelcast client kerberos generated service token failed", e);
        }

        return credentials;
    }

    @Override
    public void destroy()
    {
        kerberosAuthenticator = null;
    }
}
