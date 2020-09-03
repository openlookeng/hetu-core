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

public class KerberosConfig
{
    private static boolean kerberosEnabled;
    private static String servicePrincipalName;
    private static String loginContextName;

    private KerberosConfig() {}

    /**
     * Kerberos authentication is enabled or not
     *
     * @return true or false
     */
    public static boolean isKerberosEnabled()
    {
        return kerberosEnabled;
    }

    /**
     * Enable kerberos or not
     *
     * @param enabled true or false
     * @return
     */
    public static void setKerberosEnabled(boolean enabled)
    {
        kerberosEnabled = enabled;
    }

    /**
     * Get the service principal name
     *
     * @return The service principal
     */
    public static String getServicePrincipalName()
    {
        return servicePrincipalName;
    }

    /**
     * Set the service principal name
     *
     * @param principalName The service principal name
     * @return
     */
    public static void setServicePrincipalName(String principalName)
    {
        servicePrincipalName = principalName;
    }

    /**
     * Get the login context name
     *
     * @return Login context name
     */
    public static String getLoginContextName()
    {
        return loginContextName;
    }

    /**
     * Set the login context name
     *
     * @param contextName The context name
     * @return
     */
    public static void setLoginContextName(String contextName)
    {
        loginContextName = contextName;
    }
}
