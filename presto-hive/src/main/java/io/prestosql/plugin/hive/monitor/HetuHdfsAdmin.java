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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;
import java.net.URI;

public class HetuHdfsAdmin
{
    private final DistributedFileSystem dfs;

    /**
     * Create a new HdfsAdmin client.
     *
     * @param uri  the unique URI of the HDFS file system to administer
     * @param conf configuration
     * @throws IOException in the event the file system could not be created
     */
    public HetuHdfsAdmin(URI uri, Configuration conf) throws IOException
    {
        FileSystem fs = FileSystem.get(uri, conf);
        if (fs instanceof DistributedFileSystem) {
            dfs = (DistributedFileSystem) fs;
            return;
        }

        if (fs instanceof FilterFileSystem && ((FilterFileSystem) fs).getRawFileSystem() instanceof DistributedFileSystem) {
            dfs = (DistributedFileSystem) ((FilterFileSystem) fs).getRawFileSystem();
            return;
        }

        throw new IllegalArgumentException("'" + uri + "' is not an HDFS URI.");
    }

    /**
     * Exposes a stream of namesystem events. Only events occurring after the
     * stream is created are available.
     * See {@link org.apache.hadoop.hdfs.DFSInotifyEventInputStream}
     * for information on stream usage.
     * See {@link org.apache.hadoop.hdfs.inotify.Event}
     * for information on the available events.
     * <p>
     * Inotify users may want to tune the following HDFS parameters to
     * ensure that enough extra HDFS edits are saved to support inotify clients
     * that fall behind the current state of the namespace while reading events.
     * The default parameter values should generally be reasonable. If edits are
     * deleted before their corresponding events can be read, clients will see a
     * {@link org.apache.hadoop.hdfs.inotify.MissingEventsException} on
     * {@link org.apache.hadoop.hdfs.DFSInotifyEventInputStream} method calls.
     *
     * It should generally be sufficient to tune these parameters:
     * dfs.namenode.num.extra.edits.retained
     * dfs.namenode.max.extra.edits.segments.retained
     *
     * Parameters that affect the number of created segments and the number of
     * edits that are considered necessary, i.e. do not count towards the
     * dfs.namenode.num.extra.edits.retained quota):
     * dfs.namenode.checkpoint.period
     * dfs.namenode.checkpoint.txns
     * dfs.namenode.num.checkpoints.retained
     * dfs.ha.log-roll.period
     * <p>
     * It is recommended that local journaling be configured
     * (dfs.namenode.edits.dir) for inotify (in addition to a shared journal)
     * so that edit transfers from the shared journal can be avoided.
     *
     * @throws IOException If there was an error obtaining the stream.
     */
    public DFSInotifyEventInputStream getInotifyEventStream() throws IOException
    {
        return dfs.getInotifyEventStream();
    }

    /**
     * A version of {@link HdfsAdmin#getInotifyEventStream()} meant for advanced
     * users who are aware of HDFS edits up to lastReadTxid (e.g. because they
     * have access to an FSImage inclusive of lastReadTxid) and only want to read
     * events after this point.
     */
    public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
            throws IOException
    {
        return dfs.getInotifyEventStream(lastReadTxid);
    }
}
