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
import io.airlift.log.Logger;
import io.prestosql.server.remotetask.MaxRetryBackoff;
import io.prestosql.spi.HostAddress;

public class MaxRetryFailureRetryPolicy
                extends AbstractFailureRetryPolicy
{
    private static final Logger log = Logger.get(MaxRetryFailureRetryPolicy.class);

    private MaxRetryFailureRetryConfig config;
    FailureDetector failureDetector;

    public MaxRetryFailureRetryPolicy(MaxRetryFailureRetryConfig config)
    {
        super(new MaxRetryBackoff(config.getMaxRetryCount()));
        this.failureDetector = FailureDetectorManager.getDefaultFailureDetector();
        this.config = config;
    }

    public MaxRetryFailureRetryPolicy(MaxRetryFailureRetryConfig config, Ticker ticker)
    {
        super(new MaxRetryBackoff(config.getMaxRetryCount(), ticker));
        this.failureDetector = FailureDetectorManager.getDefaultFailureDetector();
        this.config = config;
    }

    @Override
    public boolean hasFailed(HostAddress address)
    {
        FailureDetector.State remoteHostState = failureDetector.getState(address);
        log.debug("remote node state is: " + remoteHostState.toString());
        return (((MaxRetryBackoff) getBackoff()).maxRetryDone() && !FailureDetector.State.ALIVE.equals(remoteHostState))
                        || ((MaxRetryBackoff) getBackoff()).timeout();
    }
}
