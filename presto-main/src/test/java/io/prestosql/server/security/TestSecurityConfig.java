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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static io.prestosql.server.security.SecurityConfig.AuthenticationType.KERBEROS;
import static io.prestosql.server.security.SecurityConfig.AuthenticationType.PASSWORD;

public class TestSecurityConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(SecurityConfig.class)
                .setAuthenticationTypes("")
                .setHttpHeaderCsp("object-src 'none'")
                .setHttpHeaderRp("strict-origin-when-cross-origin")
                .setHttpHeaderXcto("nosniff")
                .setHttpHeaderXfo("deny")
                .setHttpHeaderXpcdp("master-only")
                .setHttpHeaderXxp("1; mode=block"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http-server.authentication.type", "KERBEROS,PASSWORD")
                .put("http-header.content-security-policy", "script-src 'self'")
                .put("http-header.referrer-policy", "origin")
                .put("http-header.x-content-type-options", "none")
                .put("http-header.x-frame-options", "none")
                .put("http-header.x-permitted-cross-domain-policies", "sameorigin")
                .put("http-header.x-xss-protection", "0")
                .build();

        SecurityConfig expected = new SecurityConfig()
                .setAuthenticationTypes(ImmutableList.of(KERBEROS, PASSWORD))
                .setHttpHeaderCsp("script-src 'self'")
                .setHttpHeaderRp("origin")
                .setHttpHeaderXcto("none")
                .setHttpHeaderXfo("none")
                .setHttpHeaderXpcdp("sameorigin")
                .setHttpHeaderXxp("0");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
