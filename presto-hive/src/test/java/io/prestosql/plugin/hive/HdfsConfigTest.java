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
package io.prestosql.plugin.hive;

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.util.validation.FileExistsUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;

public class HdfsConfigTest
{
    private HdfsConfig hdfsConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hdfsConfigUnderTest = new HdfsConfig();
    }

    @Test
    public void testGetResourceConfigFiles()
    {
        List<@FileExistsUtil File> resourceConfigFiles = hdfsConfigUnderTest.getResourceConfigFiles();
        HdfsConfig hdfsConfig = hdfsConfigUnderTest.setResourceConfigFiles(resourceConfigFiles);
    }

    @Test
    public void testSetResourceConfigFiles()
    {
        hdfsConfigUnderTest.setResourceConfigFiles("/file");
    }

    @Test
    public void testGetNewDirectoryFsPermissions()
    {
        // Setup
        final Optional<FsPermission> expectedResult = Optional.of(
                new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE, false));

        // Run the test
        final Optional<FsPermission> result = hdfsConfigUnderTest.getNewDirectoryFsPermissions();
    }

    @Test
    public void testGetNewDirectoryPermissions()
    {
        String newDirectoryPermissions = hdfsConfigUnderTest.getNewDirectoryPermissions();
        hdfsConfigUnderTest.setNewDirectoryPermissions(newDirectoryPermissions);
    }

    @Test
    public void testIsNewFileInheritOwnership()
    {
        boolean newFileInheritOwnership = hdfsConfigUnderTest.isNewFileInheritOwnership();
        hdfsConfigUnderTest.setNewFileInheritOwnership(newFileInheritOwnership);
    }

    @Test
    public void testIsVerifyChecksum()
    {
        boolean newFileInheritOwnership = hdfsConfigUnderTest.isVerifyChecksum();
        hdfsConfigUnderTest.setVerifyChecksum(newFileInheritOwnership);
    }

    @Test
    public void testGetIpcPingInterval()
    {
        Duration ipcPingInterval = hdfsConfigUnderTest.getIpcPingInterval();
        hdfsConfigUnderTest.setIpcPingInterval(ipcPingInterval);
    }

    @Test
    public void testGetDfsTimeout()
    {
        Duration ipcPingInterval = hdfsConfigUnderTest.getDfsTimeout();
        hdfsConfigUnderTest.setDfsTimeout(ipcPingInterval);
    }

    @Test
    public void testGetDfsConnectTimeout()
    {
        Duration ipcPingInterval = hdfsConfigUnderTest.getDfsConnectTimeout();
        hdfsConfigUnderTest.setDfsConnectTimeout(ipcPingInterval);
    }

    @Test
    public void testGetDfsConnectMaxRetries()
    {
        int dfsConnectMaxRetries = hdfsConfigUnderTest.getDfsConnectMaxRetries();
        hdfsConfigUnderTest.setDfsConnectMaxRetries(dfsConnectMaxRetries);
    }

    @Test
    public void testGetDfsKeyProviderCacheTtl()
    {
        Duration ipcPingInterval = hdfsConfigUnderTest.getDfsKeyProviderCacheTtl();
        hdfsConfigUnderTest.setDfsKeyProviderCacheTtl(ipcPingInterval);
    }

    @Test
    public void testGetDomainSocketPath()
    {
        String domainSocketPath = hdfsConfigUnderTest.getDomainSocketPath();
        hdfsConfigUnderTest.setDomainSocketPath(domainSocketPath);
    }

    @Test
    public void testGetSocksProxy()
    {
        HostAndPort socksProxy = hdfsConfigUnderTest.getSocksProxy();
        hdfsConfigUnderTest.setSocksProxy(socksProxy);
    }

    @Test
    public void testIsWireEncryptionEnabled()
    {
        boolean wireEncryptionEnabled = hdfsConfigUnderTest.isWireEncryptionEnabled();
        hdfsConfigUnderTest.setWireEncryptionEnabled(wireEncryptionEnabled);
    }

    @Test
    public void testGetFileSystemMaxCacheSize()
    {
        int fileSystemMaxCacheSize = hdfsConfigUnderTest.getFileSystemMaxCacheSize();
        hdfsConfigUnderTest.setFileSystemMaxCacheSize(fileSystemMaxCacheSize);
    }

    @Test
    public void testGetDfsReplication()
    {
        int fileSystemMaxCacheSize = hdfsConfigUnderTest.getDfsReplication();
        hdfsConfigUnderTest.setDfsReplication(fileSystemMaxCacheSize);
    }
}
