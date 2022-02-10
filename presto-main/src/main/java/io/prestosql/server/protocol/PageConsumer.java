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

import java.util.concurrent.BlockingQueue;

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

    public boolean isFinished()
    {
        return this.stopped || (this.lastResult == null &&
                (this.state == State.FINISHED || this.state == State.ERROR) &&
                // no more requests expected or sent final result and it's been more than 30 sec
                (this.expectNoMoreRequests || (this.sentFinalStatus && (System.currentTimeMillis() - this.lastSubscriberTime <= 30_000))));
    }

    public boolean isActive()
    {
        // no subscribers in last pageConsumerTimeout milliseconds
        return System.currentTimeMillis() - this.lastSubscriberTime <= this.pageConsumerTimeout;
    }

    private DataCenterQueryResults getResult(long token, BlockingQueue<DataCenterQueryResults> queryResults)
    {
        if (token == (lastToken + 1)) {
            // Current result
            lastResult = queryResults.poll();

            if (lastResult == null) {
                if (this.sentFinalStatus && (token == (lastToken + 1))) {
                    this.expectNoMoreRequests = true;
                }
                if (this.state == State.FINISHED) {
                    // this query is finished, return finished.
                    lastResult = this.standardFinished;
                    this.sentFinalStatus = true;
                }
                else if (this.state == State.ERROR) {
                    // this query state is error, return failed.
                    lastResult = this.standardFailed;
                    this.sentFinalStatus = true;
                }
                else {
                    // queryResults is empty and this query is running, return running.
                    lastResult = this.standardRunning;
                }
            }
            else {
                if (lastResult.getNextUri() == null) {
                    this.sentFinalStatus = true;
                }
            }
            lastToken = token;
            return lastResult;
        }

        // is the a repeated request for the last results?
        if (token == lastToken) {
            if (lastResult == null) {
                return this.standardRunning;
            }
            return lastResult;
        }

        // if this is a result before the lastResult, the data is gone
        if (token < lastToken) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        throw new WebApplicationException(Response.Status.NOT_FOUND);
    }

    public void add(Query query, PageSubscriber subscriber, BlockingQueue<DataCenterQueryResults> queryResults)
    {
        if (this.query == null && query != null) {
            this.query = query;
        }
        long clientToken = subscriber.getToken();
        this.lastSubscriberTime = System.currentTimeMillis();
        subscriber.send(this.query, getResult(clientToken, queryResults));
    }

    public void stop()
    {
        this.stopped = true;
    }

    public void setState(Query query, State state)
    {
        this.query = query;
        if (this.state == State.RUNNING) {
            this.state = state;
        }
    }
}
