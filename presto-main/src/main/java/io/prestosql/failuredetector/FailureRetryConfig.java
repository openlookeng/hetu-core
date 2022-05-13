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
package io.prestosql.failuredetector;

import io.airlift.configuration.Config;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import javax.validation.constraints.NotNull;

public class FailureRetryConfig
{
    public static final String DEFAULT_CONFIG_NAME = "default";
    private String failureRetryPolicyConfig = DEFAULT_CONFIG_NAME;

    @Config(FailureRetryPolicy.FD_RETRY_PROFILE)
    public FailureRetryConfig setFailureRetryPolicyProfile(String profileName)
    {
        this.failureRetryPolicyConfig = profileName;
        return this;
    }

    @NotNull
    public String getFailureRetryPolicyProfile()
    {
        return failureRetryPolicyConfig;
    }
}
