/*
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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.queryeditorui.QueryEditorConfig;
import io.prestosql.queryeditorui.security.UiAuthenticator;
import io.prestosql.server.InternalAuthenticationManager;
import io.prestosql.server.security.Authenticator.AuthenticatedPrincipal;
import io.prestosql.spi.security.BasicPrincipal;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class AuthenticationFilter
        implements Filter
{
    private final List<Authenticator> authenticators;
    private final InternalAuthenticationManager internalAuthenticationManager;
    private final WebUIAuthenticator uiAuthenticator;
    private final QueryEditorConfig config;

    @Inject
    public AuthenticationFilter(List<Authenticator> authenticators,
                InternalAuthenticationManager internalAuthenticationManager,
                WebUIAuthenticator uiAuthenticator,
                QueryEditorConfig config)
    {
        this.authenticators = ImmutableList.copyOf(authenticators);
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        this.uiAuthenticator = uiAuthenticator;
        this.config = config;
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (internalAuthenticationManager.isInternalRequest(request)) {
            Principal principal = internalAuthenticationManager.authenticateInternalRequest(request);
            if (principal == null) {
                response.sendError(SC_UNAUTHORIZED);
                return;
            }
            nextFilter.doFilter(withPrincipal(request, principal), response);
            return;
        }

        if (isWebUi(request)) {
            // asset files, vendor files and disable page are always visible
            if (isSkipAuth(request)) {
                nextFilter.doFilter(request, response);
                return;
            }

            Optional<String> authenticatedUser = uiAuthenticator.getAuthenticatedUsername(request);
            if (authenticatedUser.isPresent()) {
                // if the authenticated user is requesting the login page, send them directly to the ui
                if (request.getPathInfo().equals(UiAuthenticator.LOGIN_FORM)) {
                    response.sendRedirect(UiAuthenticator.UI_LOCATION);
                    return;
                }
                // authentication succeeded
                request.setAttribute(PRESTO_USER, authenticatedUser.get());
                nextFilter.doFilter(withPrincipal(request, new BasicPrincipal(authenticatedUser.get())), response);
                return;
            }

            AccessType accessType = getAccessType(request, authenticators, config);
            if (accessType.equals(AccessType.DISABLE)) {
                // redirect to disable page
                response.sendRedirect(UiAuthenticator.DISABLED_LOCATION);
                return;
            }

            // skip authentication for login/logout page
            if (isLoginLogout(request)) {
                nextFilter.doFilter(request, response);
                return;
            }

            if (accessType.equals(AccessType.REDIRECT)) {
                // redirect to login page
                URI redirectUri = UiAuthenticator.buildLoginFormURI(URI.create(request.getRequestURI()));
                response.sendRedirect(redirectUri.toString());
                return;
            }
        }

        // skip authentication if non-secure or not configured
        if (!request.isSecure() || authenticators.isEmpty()) {
            nextFilter.doFilter(request, response);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            AuthenticatedPrincipal authenticatedPrincipal;
            try {
                authenticatedPrincipal = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }

            // authentication succeeded
            request.setAttribute(PRESTO_USER, authenticatedPrincipal.getUser());
            nextFilter.doFilter(withPrincipal(request, authenticatedPrincipal.getPrincipal()), response);
            return;
        }

        // authentication failed
        skipRequestBody(request);

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        response.sendError(SC_UNAUTHORIZED, Joiner.on(" | ").join(messages));
    }

    public static AccessType getAccessType(HttpServletRequest request,
                final List<Authenticator> authenticators,
                QueryEditorConfig config)
    {
        // secured requests
        if (request.isSecure()) {
            boolean pwdAuthentication = false;
            boolean kerberosAuthentication = false;

            for (Authenticator authenticator : authenticators) {
                if (authenticator instanceof PasswordAuthenticator) {
                    pwdAuthentication = true;
                }
                else if (authenticator instanceof KerberosAuthenticator) {
                    kerberosAuthentication = true;
                }
            }

            // Don't enable pwdAuthentication or kerberosAuthentication
            if (!pwdAuthentication && !kerberosAuthentication) {
                return AccessType.DISABLE;
            }

            // request path "/" needs to redirect to login page
            if (isWebUri(request)) {
                return AccessType.REDIRECT;
            }

            // Support kerberos login. When enable kerberosAuthentication, don't need to redirect to login page
            if (kerberosAuthentication) {
                return AccessType.DIRECT;
            }

            return AccessType.REDIRECT;
        }
        // unsecured requests support username-only authentication
        if (config.isAllowInsecureOverHttp()) {
            return AccessType.REDIRECT;
        }
        return AccessType.DISABLE;
    }

    public static boolean isWebUri(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        return pathInfo.equals("/");
    }

    public static boolean isWebUi(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        String referer = request.getHeader("referer");
        boolean isWebUi = pathInfo.equals("/") ||
                pathInfo.equals("/ui") ||
                pathInfo.matches("/ui/.*") ||
                referer != null && referer.matches(".*/ui/.*");
        return isWebUi;
    }

    private boolean isLoginLogout(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        return pathInfo.matches("/ui/login.html.*") ||
                pathInfo.matches("/ui/api/login.*") && request.getMethod().equals("POST") ||
                pathInfo.matches("/ui/api/logout.*") && request.getMethod().equals("POST");
    }

    private boolean isSkipAuth(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        return pathInfo.matches("/ui/disabled.html.*") ||
                pathInfo.matches("/ui/assets/.*") ||
                pathInfo.matches("/ui/vendor/.*");
    }

    private static ServletRequest withPrincipal(HttpServletRequest request, Principal principal)
    {
        requireNonNull(principal, "principal is null");
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal;
            }
        };
    }

    private static void skipRequestBody(HttpServletRequest request)
            throws IOException
    {
        // If we send the challenge without consuming the body of the request,
        // the server will close the connection after sending the response.
        // The client may interpret this as a failed request and not resend the
        // request with the authentication header. We can avoid this behavior
        // in the client by reading and discarding the entire body of the
        // unauthenticated request before sending the response.
        try (InputStream inputStream = request.getInputStream()) {
            copy(inputStream, nullOutputStream());
        }
    }

    private enum AccessType
    {
        DISABLE, REDIRECT, DIRECT;
    }
}
