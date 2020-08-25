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
package io.hetu.core.eventlistener.listeners;

import io.hetu.core.eventlistener.HetuEventListenerConfig;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

import static java.util.Objects.requireNonNull;

public abstract class BaseEventListener
        implements EventListener
{
    /**
     * An event listener that does nothing.
     */
    public static final EventListener SILENT_EVENT_LISTENER = new EventListener()
    {
        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            // Do nothing
        }
    };

    private final boolean isListenQueryCreation;

    private final boolean isListenQueryCompletion;

    private final boolean isListenSplitCompletion;

    BaseEventListener(HetuEventListenerConfig config)
    {
        requireNonNull(config, "config is null");
        this.isListenQueryCreation = config.isListenQueryCreation();
        this.isListenQueryCompletion = config.isListenQueryCompletion();
        this.isListenSplitCompletion = config.isListenSplitCompletion();
    }

    /**
     * create an listener
     *
     * @param config config object from property file
     * @return event listener
     */
    public static EventListener create(HetuEventListenerConfig config)
    {
        Type type = config.getType();

        if (type == Type.AUDIT) {
            return new AuditEventLogger(config);
        }

        if (type == null || (!config.isListenQueryCreation() && !config.isListenQueryCompletion()
                && !config.isListenSplitCompletion())) {
            // Do not listen anything
            return SILENT_EVENT_LISTENER;
        }

        if (type == Type.LOGGER) {
            return new QueryEventLogger(config);
        }
        else {
            throw new IllegalArgumentException(
                    "Hetu event listener does not support the type: " + config.getType());
        }
    }

    @Override
    public final void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (this.isListenQueryCreation) {
            this.onQueryCreatedEvent(queryCreatedEvent);
        }
    }

    @Override
    public final void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (this.isListenQueryCompletion) {
            this.onQueryCompletedEvent(queryCompletedEvent);
        }
    }

    @Override
    public final void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (this.isListenSplitCompletion) {
            this.onSplitCompletedEvent(splitCompletedEvent);
        }
    }

    /**
     * call back for query created
     *
     * @param queryCreatedEvent event object
     */
    protected void onQueryCreatedEvent(QueryCreatedEvent queryCreatedEvent)
    {
    }

    /**
     * call back for query completed
     *
     * @param queryCompletedEvent event object
     */
    protected void onQueryCompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
    }

    /**
     * call back for split completed
     *
     * @param splitCompletedEvent event object
     */
    protected void onSplitCompletedEvent(SplitCompletedEvent splitCompletedEvent)
    {
    }

    public enum Type
    {
        /**
         * logger enum
         */
        LOGGER,
        AUDIT
    }
}
