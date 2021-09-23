/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.server;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Locale;

/**
 * This class check PrestoServer where startup complete.
 */
public class HttpServerAvailableCheckFilter
        extends HttpFilter
{
    private ServerInfoResource serverInfoResource;

    @Inject
    public HttpServerAvailableCheckFilter(ServerInfoResource serverInfoResource)
    {
        this.serverInfoResource = serverInfoResource;
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
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        if (!serverInfoResource.isStartupComplete()) {
            String method = httpServletRequest.getMethod().toUpperCase(Locale.ENGLISH);
            String url = httpServletRequest.getRequestURI();
            // TODO: ISSUE-ID I3NGLB, wait for discovery policy decision
            if ("POST".equals(method) && url != null && url.contains("/v1/task/")) {
                httpServletResponse.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                return;
            }
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }
}
