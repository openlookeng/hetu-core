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
package io.prestosql.queryeditorui.security;

import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.queryeditorui.QueryEditorConfig;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.server.security.WebUIAuthenticator;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.PasswordAuthenticator;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class UiAuthenticator
        implements WebUIAuthenticator
{
    private static final String PRESTO_UI_AUDIENCE = "presto-ui";
    private static final String PRESTO_UI_COOKIE = "Presto-UI-Token";
    public static final String LOGIN_FORM = "/ui/login.html";
    public static final URI LOGIN_FORM_URI = URI.create(LOGIN_FORM);
    public static final String DISABLED_LOCATION = "/ui/disabled.html";
    public static final URI DISABLED_LOCATION_URI = URI.create(DISABLED_LOCATION);
    public static final String UI_LOCATION = "/ui/";
    private static final URI UI_LOCATION_URI = URI.create(UI_LOCATION);

    private PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final Function<String, String> jwtParser;
    private final Function<String, String> jwtGenerator;
    private final QueryEditorConfig config;

    @Inject
    public UiAuthenticator(QueryEditorConfig config, PasswordAuthenticatorManager passwordAuthenticatorManager)
    {
        byte[] hmac;
        if (config.getSharedSecret().isPresent()) {
            hmac = Hashing.sha256().hashString(config.getSharedSecret().get(), UTF_8).asBytes();
        }
        else {
            hmac = new byte[32];
            new SecureRandom().nextBytes(hmac);
        }

        this.config = config;
        this.jwtParser = jwt -> parseJwt(hmac, jwt);

        long sessionTimeoutNanos = config.getSessionTimeout().roundTo(NANOSECONDS);
        this.jwtGenerator = username -> generateJwt(hmac, username, sessionTimeoutNanos);

        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
    }

    /**
     * Below code has been borrowed from prestosql's FormWebUiAuthenticationFilter.java
     */

    public static URI buildLoginFormURI(URI requestUri)
    {
        UriBuilder builder = UriBuilder.fromUri(requestUri)
                .uri(LOGIN_FORM_URI);

        String path = requestUri.getPath();
        if (!path.startsWith("/ui")) {
            //No need to redirect API calls
            return builder.build();
        }
        if (!isNullOrEmpty(requestUri.getQuery())) {
            path += "?" + requestUri.getQuery();
        }

        if (path.equals("/ui") || path.equals("/ui/")) {
            return builder.build();
        }

        // this is a hack - the replaceQuery method encodes the value where the uri method just copies the value
        try {
            builder.uri(new URI(null, null, null, path, null));
        }
        catch (URISyntaxException ignored) {
            // could be ignored
        }

        return builder.build();
    }

    public static Response.ResponseBuilder redirectFromSuccessfulLoginResponse(String inputRedirectPath)
    {
        String redirectPath = inputRedirectPath;
        URI redirectLocation = UI_LOCATION_URI;

        redirectPath = emptyToNull(redirectPath);
        if (redirectPath != null) {
            try {
                redirectLocation = new URI(redirectPath);
            }
            catch (URISyntaxException ignored) {
                // could be ignored
            }
        }

        return Response.seeOther(redirectLocation);
    }

    @Override
    public Optional<NewCookie> checkLoginCredentials(String username, String password, boolean secure)
    {
        if (isValidCredential(username, password, secure)) {
            return Optional.of(createAuthenticationCookie(username, secure));
        }
        return Optional.empty();
    }

    private boolean isValidCredential(String username, String password, boolean secure)
    {
        if (username == null) {
            return false;
        }

        if (!secure) {
            return config.isAllowInsecureOverHttp() && password == null;
        }

        PasswordAuthenticator authenticator = passwordAuthenticatorManager.getAuthenticator();
        try {
            authenticator.createAuthenticatedPrincipal(username, password);
            return true;
        }
        catch (AccessDeniedException e) {
            return false;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public Optional<String> getAuthenticatedUsername(HttpServletRequest request)
    {
        javax.servlet.http.Cookie[] cookies = request.getCookies();
        javax.servlet.http.Cookie cookie = cookies == null ? null : Arrays.stream(cookies)
                .filter(c -> PRESTO_UI_COOKIE.equals(c.getName()))
                .findFirst().orElse(null);
        if (cookie == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(jwtParser.apply(cookie.getValue()));
        }
        catch (JwtException e) {
            return Optional.empty();
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private NewCookie createAuthenticationCookie(String userName, boolean secure)
    {
        String jwt = jwtGenerator.apply(userName);
        return new NewCookie(
                PRESTO_UI_COOKIE,
                jwt,
                "/",
                null,
                Cookie.DEFAULT_VERSION,
                null,
                NewCookie.DEFAULT_MAX_AGE,
                null,
                secure,
                true);
    }

    public static NewCookie getDeleteCookie(boolean secure)
    {
        return new NewCookie(
                PRESTO_UI_COOKIE,
                "delete",
                "/",
                null,
                Cookie.DEFAULT_VERSION,
                null,
                0,
                null,
                secure,
                true);
    }

    private static String generateJwt(byte[] hmac, String username, long sessionTimeoutNanos)
    {
        return Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, hmac)
                .setSubject(username)
                .setExpiration(Date.from(ZonedDateTime.now().plusNanos(sessionTimeoutNanos).toInstant()))
                .setAudience(PRESTO_UI_AUDIENCE)
                .compact();
    }

    private static String parseJwt(byte[] hmac, String jwt)
    {
        return Jwts.parser()
                .setSigningKey(hmac)
                .requireAudience(PRESTO_UI_AUDIENCE)
                .parseClaimsJws(jwt)
                .getBody()
                .getSubject();
    }
}
