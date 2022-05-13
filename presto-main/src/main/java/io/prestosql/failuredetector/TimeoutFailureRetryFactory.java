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

import com.google.common.base.Ticker;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import java.util.Properties;

public class TimeoutFailureRetryFactory
        extends FailureRetryPolicyFactory
{
    @Override
    public AbstractFailureRetryPolicy getFailureRetryPolicy(Properties properties)
    {
        return new TimeoutFailureRetryPolicy(new TimeoutFailureRetryConfig(properties));
    }

    @Override
    public AbstractFailureRetryPolicy getFailureRetryPolicy(Properties properties, Ticker ticker)
    {
        return new TimeoutFailureRetryPolicy(new TimeoutFailureRetryConfig(properties), ticker);
    }

    @Override
    public String getName()
    {
        return FailureRetryPolicy.TIMEOUT;
    }
}
