/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive.monitor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.ForHdfsMonitor;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.MoreFutures.whenAnyComplete;

public class HdfsStorageMonitor
{
    private static final Logger log = Logger.get(HdfsStorageMonitor.class);

    private final HdfsEnvironment hdfsEnvironment;
    private final ListeningExecutorService executorService;
    private final HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(new ConnectorIdentity("openLooKeng", Optional.empty(), Optional.empty()));

    private final Map<String, Monitor> monitoredTables = new ConcurrentHashMap<>();
    private final Map<String, ListenableFuture<Monitor>> waitingWatchers = new ConcurrentHashMap<>();
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final SettableFuture<Void> shutdown = SettableFuture.create();

    private class Monitor
            implements Callable
    {
        private final String path;
        private final HetuHdfsAdmin admin;
        private final AtomicLong refCount = new AtomicLong(1);
        private final AtomicLong lastTxnId = new AtomicLong();
        private DFSInotifyEventInputStream eventStream;
        private final AtomicInteger listeners = new AtomicInteger(1);

        public Monitor(String path, HetuHdfsAdmin admin, HdfsEnvironment.HdfsContext context) throws IOException
        {
            this.path = path;
            this.admin = admin;

            eventStream = admin.getInotifyEventStream();
        }

        public String getPath()
        {
            return path;
        }

        public HetuHdfsAdmin getAdmin()
        {
            return admin;
        }

        public AtomicLong getRefCount()
        {
            return refCount;
        }

        public int incrementListener()
        {
            return this.listeners.incrementAndGet();
        }

        public int decrementListener()
        {
            return this.listeners.decrementAndGet();
        }

        public int getListeners()
        {
            return this.listeners.get();
        }

        @Override
        public Monitor call() throws Exception
        {
            return hdfsEnvironment.doAs(context.getIdentity().getUser(), () -> {
                EventBatch events;
                try {
                    events = eventStream.poll(5, TimeUnit.SECONDS);
                    lastTxnId.set(events.getTxid());
                }
                catch (MissingEventsException me) {
                    log.info("missed hdfs notification transaction id: %d, expected txn: %d",
                            me.getActualTxid(), me.getExpectedTxid());

                    eventStream = admin.getInotifyEventStream(lastTxnId.get());
                    return this;
                }

                for (Event event : events.getEvents()) {
                    switch (event.getEventType()) {
                        case CREATE:
                            Event.CreateEvent createEvent = (Event.CreateEvent) event;
                            log.debug("file added: %s", createEvent.getPath());
                            refCount.incrementAndGet();
                            break;

                        case RENAME:
                            Event.RenameEvent renameEvent = (Event.RenameEvent) event;
                            log.debug("file renamed: %s", renameEvent.getDstPath());
                            refCount.incrementAndGet();
                            break;

                        /* Todo: TRUNCATE, UNLINK : disambiguate Vacuum from normal operations */
                        default:
                            break;
                    }
                }

                return this;
            });
        }
    }

    @Inject
    public HdfsStorageMonitor(HdfsEnvironment hdfsEnvironment,
                              @ForHdfsMonitor ListeningExecutorService executorService)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.executorService = executorService;

        executorService.submit(() -> {
            try {
                monitorForTable();
            }
            catch (ExecutionException e) {
                log.info("Exception in starting monitor! exception: %s", e.getMessage());
            }
            catch (InterruptedException e) {
                log.info("Exception in starting monitor! exception: %s", e.getMessage());
            }
        });
    }

    public synchronized long registerLocationForMonitoring(String location, ConnectorSession session) throws IOException, InterruptedException
    {
        /* Todo: validate path if exists..
        *   also, this should only be done for non-txn tables
        *   match writeId for txn tables
        */
        if (monitoredTables.containsKey(location)) {
            Monitor monitor = monitoredTables.get(location);
            monitor.incrementListener();

            return monitor.getRefCount().get();
        }

        return hdfsEnvironment.doAs(context.getIdentity().getUser(), () -> {
            HetuHdfsAdmin admin = new HetuHdfsAdmin(URI.create(location), hdfsEnvironment.getConfiguration(context, new org.apache.hadoop.fs.Path(location)));
            Monitor monitor = new Monitor(location, admin, context);
            monitoredTables.put(location, monitor);
            waitingWatchers.put(location, executorService.submit(monitor));

            return monitor;
        }).getRefCount().get();
    }

    public synchronized void unregisterLocationForMonitoring(String location)
    {
        if (monitoredTables.containsKey(location)) {
            if (monitoredTables.get(location).decrementListener() <= 0) {
                if (waitingWatchers.containsKey(location)) {
                    waitingWatchers.get(location).cancel(true);
                    waitingWatchers.remove(location);
                }

                monitoredTables.remove(location);
            }
        }
    }

    public long getAgeForLocation(String location)
    {
        if (monitoredTables.containsKey(location)) {
            return monitoredTables.get(location).getRefCount().get();
        }

        return 0;
    }

    public int getListenerCount(String location)
    {
        if (monitoredTables.containsKey(location)) {
            return monitoredTables.get(location).getListeners();
        }

        return 0;
    }

    public void shutdown() throws ExecutionException, InterruptedException
    {
        done.set(true);
        shutdown.get();
    }

    private void monitorForTable() throws ExecutionException, InterruptedException
    {
        while (!done.get()) {
            if (waitingWatchers.size() == 0) {
                Thread.sleep(2000);
                continue;
            }
            synchronized (this) {
                Future<Monitor> finished = whenAnyComplete(waitingWatchers.values());
                Monitor monitor = finished.get();
                if (monitoredTables.containsKey(monitor.getPath())) {
                    waitingWatchers.put(monitor.getPath(), executorService.submit(monitor));
                }
                else {
                    waitingWatchers.remove(monitor.getPath());
                }
            }
        }

        shutdown.set(null);
    }
}
