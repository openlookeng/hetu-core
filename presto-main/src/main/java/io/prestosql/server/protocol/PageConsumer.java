/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.server.protocol;

import io.airlift.units.Duration;
import io.prestosql.client.DataCenterQueryResults;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class PageConsumer
{
    enum State
    {
        RUNNING, FINISHED, ERROR
    }

    private final DataCenterQueryResults standardRunning;
    private final DataCenterQueryResults standardFinished;
    private final DataCenterQueryResults standardFailed;
    private final long pageConsumerTimeout;
    private DataCenterQueryResults lastResult;
    private DataCenterQueryResults currentResult;
    private long lastToken = -1;
    private State state = State.RUNNING;
    private Query query;
    private boolean sentFinalStatus;
    private boolean expectNoMoreRequests;
    private boolean stopped;
    private long lastSubscriberTime;

    public PageConsumer(DataCenterQueryResults standardRunning, DataCenterQueryResults standardFinished, DataCenterQueryResults standardFailed, Duration pageConsumerTimeout)
    {
        this.standardRunning = standardRunning;
        this.standardFinished = standardFinished;
        this.standardFailed = standardFailed;
        this.pageConsumerTimeout = pageConsumerTimeout.toMillis();
        this.lastSubscriberTime = System.currentTimeMillis();
    }

    public synchronized void consume(Query query, DataCenterQueryResults result)
    {
        if (this.currentResult == null) {
            this.query = query;
            this.currentResult = result;
        }
        else {
            throw new IllegalStateException("lastResult is not consumed yet");
        }
    }

    public synchronized boolean hasRoom()
    {
        return this.currentResult == null;
    }

    public synchronized boolean isFinished()
    {
        return this.stopped || (this.lastResult == null &&
                this.currentResult == null &&
                (this.state == State.FINISHED || this.state == State.ERROR) &&
                // no more requests expected or sent final result and it's been more than 30 sec
                (this.expectNoMoreRequests || (this.sentFinalStatus && (System.currentTimeMillis() - this.lastSubscriberTime <= 30_000))));
    }

    public boolean isActive()
    {
        // no subscribers in last pageConsumerTimeout milliseconds
        return System.currentTimeMillis() - this.lastSubscriberTime <= this.pageConsumerTimeout;
    }

    private synchronized DataCenterQueryResults getResult(long token)
    {
        // is this the first request?
        if (lastResult == null && currentResult == null) {
            if (this.sentFinalStatus && (token == (lastToken + 1))) {
                this.expectNoMoreRequests = true;
            }
            DataCenterQueryResults results;
            if (this.state == State.FINISHED) {
                results = this.standardFinished;
                this.sentFinalStatus = true;
            }
            else if (this.state == State.ERROR) {
                results = this.standardFailed;
                this.sentFinalStatus = true;
            }
            else {
                results = this.standardRunning;
            }
            lastToken = token;
            return results;
        }

        // is the a repeated request for the last results?
        if (token == lastToken) {
            if (lastResult == null) {
                throw new WebApplicationException(Response.Status.GONE);
            }
            return lastResult;
        }
        else if (token == (lastToken + 1)) {
            // Current result
            lastToken = token;
            lastResult = currentResult;
            currentResult = null;
            if (lastResult == null) {
                return this.standardRunning;
            }
            else {
                if (lastResult.getNextUri() == null) {
                    this.sentFinalStatus = true;
                }
                return lastResult;
            }
        }

        // if this is a result before the lastResult, the data is gone
        if (token < lastToken) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // token > lastToken + 1
        throw new WebApplicationException(Response.Status.NOT_FOUND);
    }

    public synchronized void add(PageSubscriber subscriber)
    {
        long clientToken = subscriber.getToken();
        this.lastSubscriberTime = System.currentTimeMillis();
        subscriber.send(this.query, getResult(clientToken));
    }

    public synchronized void stop()
    {
        this.stopped = true;
    }

    public synchronized void setState(Query query, State state)
    {
        this.query = query;
        if (this.state == State.RUNNING) {
            this.state = state;
        }
    }
}
