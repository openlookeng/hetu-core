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
package io.prestosql.plugin.hive.aws;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Response;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.util.AWSRequestMetrics;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AbstractSdkMetricsCollectorTest
{
    private AbstractSdkMetricsCollector abstractSdkMetricsCollectorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        abstractSdkMetricsCollectorUnderTest = new AbstractSdkMetricsCollector()
        {
            @Override
            protected void recordRequestCount(long count)
            {
            }

            @Override
            protected void recordRetryCount(long count)
            {
            }

            @Override
            protected void recordThrottleExceptionCount(long count)
            {
            }

            @Override
            protected void recordHttpRequestTime(Duration duration)
            {
            }

            @Override
            protected void recordClientExecutionTime(Duration duration)
            {
            }

            @Override
            protected void recordRetryPauseTime(Duration duration)
            {
            }

            @Override
            protected void recordHttpClientPoolAvailableCount(long count)
            {
            }

            @Override
            protected void recordHttpClientPoolLeasedCount(long count)
            {
            }

            @Override
            protected void recordHttpClientPoolPendingCount(long count)
            {
            }
        };
    }

    @Test
    public void testCollectMetrics()
    {
        // Setup
        DefaultRequest servicename = new DefaultRequest(AmazonWebServiceRequest.NOOP, "servicename");
        servicename.setAWSRequestMetrics(new AWSRequestMetrics());
        final Response<?> response = new Response<>(null, new HttpResponse(null, null));

        // Run the test
        abstractSdkMetricsCollectorUnderTest.collectMetrics(servicename, response);

        // Verify the results
    }
}
