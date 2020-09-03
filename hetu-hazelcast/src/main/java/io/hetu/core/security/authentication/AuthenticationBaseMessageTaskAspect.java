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
package io.hetu.core.security.authentication;

import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SimpleTokenCredentials;
import io.hetu.core.security.authentication.kerberos.KerberosAuthenticator;
import io.hetu.core.security.authentication.kerberos.KerberosException;
import io.hetu.core.security.authentication.kerberos.KerberosSecurityContext;
import io.hetu.core.security.authentication.kerberos.KerberosTokenCredentials;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.ietf.jgss.GSSException;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.security.Principal;

@Aspect
@Component
public class AuthenticationBaseMessageTaskAspect
{
    @Around("execution(* com.hazelcast.client.impl.protocol.task.AuthenticationBaseMessageTask.authenticate(SecurityContext)) && args(securityContext)")
    public AuthenticationStatus aroundAuthenticate(ProceedingJoinPoint joinPoint, SecurityContext securityContext)
    {
        Field credentialsField = null;
        try {
            credentialsField = joinPoint.getTarget().getClass().getSuperclass().getDeclaredField("credentials");
            credentialsField.setAccessible(true);
            Credentials credentials = (Credentials) credentialsField.get(joinPoint.getTarget());
            KerberosAuthenticator kerberosAuthenticator = ((KerberosSecurityContext) securityContext).getKerberosAuthenticator();
            Principal requestPrincipal = kerberosAuthenticator.doAuthenticateFilter(new KerberosTokenCredentials(((SimpleTokenCredentials) credentials).getToken()));
            if (!kerberosAuthenticator.getPrincipalFullName().equals(requestPrincipal.getName())) {
                return AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER;
            }

            return AuthenticationStatus.AUTHENTICATED;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Cann't get class[%s] field.", joinPoint.getTarget().getClass().getSuperclass().getName()));
        }
        catch (KerberosException | GSSException e) {
            return AuthenticationStatus.CREDENTIALS_FAILED;
        }
        finally {
            if (credentialsField != null) {
                credentialsField.setAccessible(false);
            }
        }
    }
}
