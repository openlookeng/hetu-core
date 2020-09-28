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
package io.prestosql.client;

/**
 * default setting
 */
public class HttpSecurityHeadersConstants
{
    /**
     * Http security header: Content-Security-Policy
     */
    public static final String HTTP_SECURITY_CSP = "Content-Security-Policy";

    /**
     * Http security header: Content-Security-Policy
     */
    public static final String HTTP_SECURITY_CSP_VALUE = "object-src 'none'";

    /**
     * Http security header: Referrer-Policy
     */
    public static final String HTTP_SECURITY_RP = "Referrer-Policy";

    /**
     * Http security header: Referrer-Policy
     */
    public static final String HTTP_SECURITY_RP_VALUE = "strict-origin-when-cross-origin";

    /**
     * Http security header: X-Content-Type-Options
     */
    public static final String HTTP_SECURITY_XCTO = "X-Content-Type-Options";

    /**
     * Http security header: X-Content-Type-Options
     */
    public static final String HTTP_SECURITY_XCTO_VALUE = "nosniff";

    /**
     * Http security header: X-Frame-Options
     */
    public static final String HTTP_SECURITY_XFO = "X-Frame-Options";

    /**
     * Http security header: X-Frame-Options
     */
    public static final String HTTP_SECURITY_XFO_VALUE = "deny";

    /**
     * Http security header: X-Permitted-Cross-Domain-Policies
     */
    public static final String HTTP_SECURITY_XPCDP = "X-Permitted-Cross-Domain-Policies";

    /**
     * Http security header: X-Permitted-Cross-Domain-Policies
     */
    public static final String HTTP_SECURITY_XPCDP_VALUE = "master-only";

    /**
     * Http security header: X-XSS-Protection
     */
    public static final String HTTP_SECURITY_XXP = "X-XSS-Protection";

    /**
     * Http security header: X-XSS-Protection
     */
    public static final String HTTP_SECURITY_XXP_VALUE = "1; mode=block";

    private HttpSecurityHeadersConstants()
    {
    }
}
