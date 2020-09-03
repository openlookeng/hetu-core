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

import com.hazelcast.security.SimpleTokenCredentials;

import static io.hetu.core.security.authentication.kerberos.KerberosConstant.KERBEROS_TOKEN_CRED;

public class KerberosTokenCredentials
        extends SimpleTokenCredentials
{
    private static final long serialVersionUID = -4861690795441075745L;

    public KerberosTokenCredentials() {}

    public KerberosTokenCredentials(byte[] token)
    {
        super(token);
    }

    @Override
    public int getClassId()
    {
        return KERBEROS_TOKEN_CRED;
    }

    @Override
    public String toString()
    {
        return "KerberosTokenCredentials [tokenLength=" + (getToken() != null ? getToken().length : 0) + "]";
    }
}
