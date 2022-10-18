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
package io.prestosql.plugin.hive.authentication;

import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HiveConfig;
import org.testng.annotations.Test;

import java.util.HashSet;

public class KerberosHadoopAuthenticationTest
{
    @Test
    public void testCreateKerberosHadoopAuthentication()
    {
        // Setup
        final KerberosAuthentication kerberosAuthentication = new KerberosAuthentication("principal", "keytabLocation");
        final HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxInitialSplits(0);
        hiveConfig.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setSplitLoaderConcurrency(0);
        hiveConfig.setMaxSplitsPerSecond(0);
        hiveConfig.setDomainCompactionThreshold(0);
        hiveConfig.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setForceLocalScheduling(false);
        hiveConfig.setMaxConcurrentFileRenames(0);
        hiveConfig.setRecursiveDirWalkerEnabled(false);
        hiveConfig.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxPartitionsPerScan(0);
        hiveConfig.setMaxOutstandingSplits(0);
        hiveConfig.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxSplitIteratorThreads(0);
        final HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(hiveConfig, new HashSet<>());

        // Run the test
        final KerberosHadoopAuthentication result = KerberosHadoopAuthentication.createKerberosHadoopAuthentication(
                kerberosAuthentication, initializer);
    }
}
