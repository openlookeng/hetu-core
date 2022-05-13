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
package io.prestosql.server.remotetask;

import com.google.common.base.Ticker;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;
import io.prestosql.spi.failuredetector.IBackoff;

public class MaxRetryBackoff
        extends Backoff
        implements IBackoff
{
    private static final Logger log = Logger.get(MaxRetryBackoff.class);
    private final int maxTries;

    MaxRetryBackoff(Duration maxFailureInterval, int maxTries)
    {
        super(maxFailureInterval);
        this.maxTries = (minTries < maxTries) ? maxTries : minTries;
    }

    public MaxRetryBackoff(int maxTries)
    {
        this(Duration.valueOf(FailureRetryPolicy.DEFAULT_TIMEOUT_DURATION), maxTries);
    }

    public MaxRetryBackoff(int maxTries, Ticker ticker)
    {
        this(Duration.valueOf(FailureRetryPolicy.DEFAULT_TIMEOUT_DURATION), maxTries, ticker);
    }

    public MaxRetryBackoff()
    {
        this(Duration.valueOf(FailureRetryPolicy.DEFAULT_TIMEOUT_DURATION), Integer.parseInt(FailureRetryPolicy.MAX_RETRY_COUNT));
    }

    public MaxRetryBackoff(Duration maxFailureInterval, int maxTries, Ticker ticker)
    {
        super(maxFailureInterval, ticker);
        this.maxTries = (minTries < maxTries) ? maxTries : minTries;
    }

    /**
     * @return true if max retry failed, now it is time to check node status from HeartbeatFailureDetector
     */

    public synchronized boolean maxRetryDone()
    {
        long now = ticker.read();

        lastFailureTime = now;
        updateFailureCount();
        if (lastRequestStart != 0) {
            failureRequestTimeTotal += now - lastRequestStart;
            lastRequestStart = 0;
        }

        if (firstFailureTime == 0) {
            firstFailureTime = now;
            // can not fail on first failure
            return false;
        }

        if (getFailureCount() < minTries) {
            return false;
        }

        log.debug(" failure count " + getFailureCount());
        if (getFailureCount() >= maxTries) {
            log.debug(" failure count has crossed max retry count " + maxTries);
        }
        return getFailureCount() >= maxTries;
    }

    /**
     * @return true if maxErrorDuration is passed. Does not matter how many retry has happened.
     */
    public synchronized boolean timeout()
    {
        long now = ticker.read();
        long failureDuration = now - firstFailureTime;
        return failureDuration >= maxFailureIntervalNanos;
    }
}
