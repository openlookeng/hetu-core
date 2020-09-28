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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.prestosql.client.HttpSecurityHeadersConstants;

import javax.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;

@DefunctConfig("http.server.authentication.enabled")
public class SecurityConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<AuthenticationType> authenticationTypes = ImmutableList.of();

    public enum AuthenticationType
    {
        CERTIFICATE,
        KERBEROS,
        PASSWORD,
        JWT
    }

    private String httpHeaderCsp = HttpSecurityHeadersConstants.HTTP_SECURITY_CSP_VALUE;
    private String httpHeaderRp = HttpSecurityHeadersConstants.HTTP_SECURITY_RP_VALUE;
    private String httpHeaderXcto = HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO_VALUE;
    private String httpHeaderXfo = HttpSecurityHeadersConstants.HTTP_SECURITY_XFO_VALUE;
    private String httpHeaderXpcdp = HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP_VALUE;
    private String httpHeaderXxp = HttpSecurityHeadersConstants.HTTP_SECURITY_XXP_VALUE;

    {
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_CSP, httpHeaderCsp);
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_RP, httpHeaderRp);
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO, httpHeaderXcto);
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XFO, httpHeaderXfo);
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP, httpHeaderXpcdp);
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XXP, httpHeaderXxp);
    }

    @NotNull
    public List<AuthenticationType> getAuthenticationTypes()
    {
        return authenticationTypes;
    }

    public SecurityConfig setAuthenticationTypes(List<AuthenticationType> authenticationTypes)
    {
        this.authenticationTypes = ImmutableList.copyOf(authenticationTypes);
        return this;
    }

    @Config("http-server.authentication.type")
    @ConfigDescription("Authentication types (supported types: CERTIFICATE, KERBEROS, PASSWORD, JWT)")
    public SecurityConfig setAuthenticationTypes(String types)
    {
        if (types == null) {
            authenticationTypes = null;
            return this;
        }

        authenticationTypes = stream(SPLITTER.split(types))
                .map(AuthenticationType::valueOf)
                .collect(toImmutableList());
        return this;
    }

    public String getHttpHeaderCsp()
    {
        return this.httpHeaderCsp;
    }

    @Config("http-header.content-security-policy")
    public SecurityConfig setHttpHeaderCsp(String httpHeaderCsp)
    {
        this.httpHeaderCsp = httpHeaderCsp;
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_CSP, httpHeaderCsp);
        return this;
    }

    public String getHttpHeaderRp()
    {
        return httpHeaderRp;
    }

    @Config("http-header.referrer-policy")
    public SecurityConfig setHttpHeaderRp(String httpHeaderRp)
    {
        this.httpHeaderRp = httpHeaderRp;
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_RP, httpHeaderRp);
        return this;
    }

    public String getHttpHeaderXcto()
    {
        return httpHeaderXcto;
    }

    @Config("http-header.x-content-type-options")
    public SecurityConfig setHttpHeaderXcto(String httpHeaderXcto)
    {
        this.httpHeaderXcto = httpHeaderXcto;
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XCTO, httpHeaderXcto);
        return this;
    }

    public String getHttpHeaderXfo()
    {
        return httpHeaderXfo;
    }

    @Config("http-header.x-frame-options")
    public SecurityConfig setHttpHeaderXfo(String httpHeaderXfo)
    {
        this.httpHeaderXfo = httpHeaderXfo;
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XFO, httpHeaderXfo);
        return this;
    }

    public String getHttpHeaderXpcdp()
    {
        return httpHeaderXpcdp;
    }

    @Config("http-header.x-permitted-cross-domain-policies")
    public SecurityConfig setHttpHeaderXpcdp(String httpHeaderXpcdp)
    {
        this.httpHeaderXpcdp = httpHeaderXpcdp;
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XPCDP, httpHeaderXpcdp);
        return this;
    }

    public String getHttpHeaderXxp()
    {
        return httpHeaderXxp;
    }

    @Config("http-header.x-xss-protection")
    public SecurityConfig setHttpHeaderXxp(String httpHeaderXxp)
    {
        this.httpHeaderXxp = httpHeaderXxp;
        System.setProperty(HttpSecurityHeadersConstants.HTTP_SECURITY_XXP, httpHeaderXxp);
        return this;
    }
}
