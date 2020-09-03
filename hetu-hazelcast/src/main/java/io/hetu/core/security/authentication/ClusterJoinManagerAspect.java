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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.Credentials;
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
public class ClusterJoinManagerAspect
{
    private static final ILogger LOGGER = Logger.getLogger(ClusterJoinManagerAspect.class);

    @Around("execution(* com.hazelcast.internal.cluster.impl.ClusterJoinManager.secureLogin(JoinRequest, Connection)) && args(joinRequest, connection)")
    public void aroundSecureLogin(ProceedingJoinPoint joinPoint, JoinRequest joinRequest, Connection connection)
    {
        Field nodeField = null;
        String endpoint = joinRequest.getAddress().getHost();
        try {
            nodeField = joinPoint.getTarget().getClass().getDeclaredField("node");
            nodeField.setAccessible(true);
            Node node = (Node) nodeField.get(joinPoint.getTarget());
            if (node.securityContext != null) {
                Credentials credentials = joinRequest.getCredentials();
                if (credentials == null) {
                    throw new SecurityException("Missing credentials in the join request.");
                }

                KerberosAuthenticator kerberosAuthenticator = ((KerberosSecurityContext) node.securityContext).getKerberosAuthenticator();
                Principal requestPrincipal = kerberosAuthenticator.doAuthenticateFilter((KerberosTokenCredentials) credentials);
                if (!kerberosAuthenticator.getPrincipalFullName().equals(requestPrincipal.getName())) {
                    throw new KerberosException(String.format("Authenticate failed for %s to join the cluster.", endpoint));
                }

                LOGGER.info(String.format("Authenticate success for %s to join the cluster.", endpoint));
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Cann't get class[%s] field.", joinPoint.getTarget().getClass().getName()));
        }
        catch (KerberosException | GSSException e) {
            throw new SecurityException(String.format("Authenticate failed for %s, cause: %s", endpoint, e.getMessage()));
        }
        finally {
            if (nodeField != null) {
                nodeField.setAccessible(false);
            }
        }
    }
}
