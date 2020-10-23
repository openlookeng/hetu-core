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
package io.prestosql.server.security;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_CSP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_CSP_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_RP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_RP_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XFO;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XFO_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP_VALUE;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XXP;
import static io.prestosql.client.HttpSecurityHeadersConstants.HTTP_SECURITY_XXP_VALUE;

public class HttpSecurityHeaderFilter
        extends HttpFilter
{
    public HttpSecurityHeaderFilter()
    {
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException
    {
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        if (System.getProperty(HTTP_SECURITY_CSP) != null && !(servletRequest instanceof HttpServletRequest && AuthenticationFilter.isWebUi((HttpServletRequest) servletRequest))) {
            httpServletResponse.setHeader(HTTP_SECURITY_CSP, System.getProperty(HTTP_SECURITY_CSP));
        }
        else {
            httpServletResponse.setHeader(HTTP_SECURITY_CSP, HTTP_SECURITY_CSP_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_RP) != null) {
            httpServletResponse.setHeader(HTTP_SECURITY_RP, System.getProperty(HTTP_SECURITY_RP));
        }
        else {
            httpServletResponse.setHeader(HTTP_SECURITY_RP, HTTP_SECURITY_RP_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XCTO) != null) {
            httpServletResponse.setHeader(HTTP_SECURITY_XCTO, System.getProperty(HTTP_SECURITY_XCTO));
        }
        else {
            httpServletResponse.setHeader(HTTP_SECURITY_XCTO, HTTP_SECURITY_XCTO_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XFO) != null) {
            httpServletResponse.setHeader(HTTP_SECURITY_XFO, System.getProperty(HTTP_SECURITY_XFO));
        }
        else {
            httpServletResponse.setHeader(HTTP_SECURITY_XFO, HTTP_SECURITY_XFO_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XPCDP) != null) {
            httpServletResponse.setHeader(HTTP_SECURITY_XPCDP, System.getProperty(HTTP_SECURITY_XPCDP));
        }
        else {
            httpServletResponse.setHeader(HTTP_SECURITY_XPCDP, HTTP_SECURITY_XPCDP_VALUE);
        }

        if (System.getProperty(HTTP_SECURITY_XXP) != null) {
            httpServletResponse.setHeader(HTTP_SECURITY_XXP, System.getProperty(HTTP_SECURITY_XXP));
        }
        else {
            httpServletResponse.setHeader(HTTP_SECURITY_XXP, HTTP_SECURITY_XXP_VALUE);
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }
}
