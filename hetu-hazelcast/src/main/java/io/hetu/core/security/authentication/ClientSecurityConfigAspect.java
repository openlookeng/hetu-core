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

import com.hazelcast.security.ICredentialsFactory;
import io.hetu.core.security.authentication.kerberos.KerberosClientCredentialsFactory;
import io.hetu.core.security.authentication.kerberos.KerberosConfig;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ClientSecurityConfigAspect
{
    @Around("execution(* com.hazelcast.client.config.ClientSecurityConfig.asCredentialsFactory(..))")
    public ICredentialsFactory aroundAsCredentialsFactory(ProceedingJoinPoint joinPoint) throws Throwable
    {
        if (KerberosConfig.isKerberosEnabled()) {
            return new KerberosClientCredentialsFactory();
        }

        Object[] parameters = joinPoint.getArgs();
        Object iCredentialsFactory = joinPoint.proceed(parameters);
        return (ICredentialsFactory) iCredentialsFactory;
    }
}
