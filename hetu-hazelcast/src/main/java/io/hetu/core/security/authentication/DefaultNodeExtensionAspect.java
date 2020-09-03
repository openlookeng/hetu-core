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

import com.hazelcast.security.SecurityContext;
import io.hetu.core.security.authentication.kerberos.KerberosConfig;
import io.hetu.core.security.authentication.kerberos.KerberosSecurityContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class DefaultNodeExtensionAspect
{
    @Around("execution(* com.hazelcast.instance.impl.DefaultNodeExtension.getSecurityContext())")
    public SecurityContext aroundGetSecurityContext(ProceedingJoinPoint joinPoint)
    {
        if (KerberosConfig.isKerberosEnabled()) {
            return new KerberosSecurityContext();
        }

        return null;
    }

    @Around("execution(* com.hazelcast.instance.impl.DefaultNodeExtension.checkSecurityAllowed())")
    public void aroundCheckSecurityAllowed(ProceedingJoinPoint joinPoint) {}
}
