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

import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import javax.validation.constraints.Min;

import java.util.Properties;

public class MaxRetryFailureRetryConfig
{
    private String maxRetryCount;

    public MaxRetryFailureRetryConfig(Properties properties)
    {
        this.maxRetryCount = properties.getProperty(FailureRetryPolicy.MAX_RETRY_COUNT);
    }

    @Min(100)
    public int getMaxRetryCount()
    {
        if (maxRetryCount == null) {
            return FailureRetryPolicy.DEFAULT_RETRY_COUNT;
        }
        int count = Integer.parseInt(maxRetryCount);
        return count;
    }
}
