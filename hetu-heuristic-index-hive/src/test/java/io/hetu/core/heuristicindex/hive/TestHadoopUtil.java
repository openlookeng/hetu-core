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
package io.hetu.core.heuristicindex.hive;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;
import org.assertj.core.util.Lists;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestHadoopUtil
{
    @Test
    public void testKerberosConfig() throws IOException
    {
        Properties properties = new Properties();
        properties.load(new FileInputStream(TestConstantsHelper.DOCKER_KERBERIZED_HIVE_PROPERTIES_FILE_LOCATION));

        // This needs to be synchronized because Kerberos authentication methods are static.
        // There might be issues when unit tests run in parallel if it's not synchronized.
        synchronized (UserGroupInformation.class) {
            // this will be thrown because the Kerberos auth server is not correct.
            // we are just testing whether the method will go through necessary set up
            // actual testing will be done during integration test
            assertThrows(KerberosAuthException.class, () -> HadoopUtil.generateHadoopConfig(properties));

            // check kerberos config
            assertTrue(UserGroupInformation.isInitialized());
            assertTrue(UserGroupInformation.isSecurityEnabled());
            UserGroupInformation.reset();
        }
    }

    @Test
    public void testGetAcidTableFiles() throws IOException
    {
        FileSystem fs = mock(FileSystem.class);
        String[] partitions = new String[]{"c3=10"};

        Path tableRootPath = new Path("/user/hive/warehouse/acid.db", "acid_table");
        Path c310Path = new Path(tableRootPath, "c3=10");
        FileStatus c310FS = new FileStatus(-1, true, -1, -1, -1, c310Path);
        when(fs.listStatus(tableRootPath)).thenReturn(new FileStatus[]{
                c310FS
        });

        Path basePath = new Path(c310Path, "base_00000013");
        FileStatus baseFS = new FileStatus(-1, true, -1, -1, -1, basePath);
        Path deleteDeltaPath = new Path(c310Path, "delete_delta_0000016_0000016_0000");
        FileStatus deleteDeltaFS = new FileStatus(-1, true, -1, -1, -1, deleteDeltaPath);
        Path deltaPath = new Path(c310Path, "delta_0000015_0000015_0000");
        FileStatus deltaFS = new FileStatus(-1, true, -1, -1, -1, deltaPath);
        when(fs.listStatus(c310Path)).thenReturn(new FileStatus[]{
                baseFS, deleteDeltaFS, deltaFS
        });

        Path baseMetadataPath = new Path(basePath, "_metadata_acid");
        FileStatus baseMetadataFS = new FileStatus(-1, false, -1, -1, -1, baseMetadataPath);
        Path baseOrcAcidVersionPath = new Path(basePath, "_orc_acid_version");
        FileStatus baseOrcAcidVersionFS = new FileStatus(-1, false, -1, -1, -1, baseOrcAcidVersionPath);
        Path baseBucketPath = new Path(basePath, "bucket_00000");
        FileStatus baseBucketFS = new FileStatus(-1, false, -1, -1, -1, baseBucketPath);
        when(fs.listStatus(basePath)).thenReturn(new FileStatus[]{
                baseMetadataFS, baseOrcAcidVersionFS, baseBucketFS
        });

        Path deltaOrcAcidVersionPath = new Path(deltaPath, "_orc_acid_version");
        FileStatus deltaOrcAcidVersionFS = new FileStatus(-1, false, -1, -1, -1, deltaOrcAcidVersionPath);
        Path deltaBucketPath = new Path(deltaPath, "bucket_00000");
        FileStatus deltaBucketFS = new FileStatus(-1, false, -1, -1, -1, deltaBucketPath);
        when(fs.listStatus(deltaPath)).thenReturn(new FileStatus[]{
                deltaOrcAcidVersionFS, deltaBucketFS
        });

        List<FileStatus> fileStatuses = HadoopUtil.getFiles(fs, tableRootPath, partitions, true);
        assertEquals(2, fileStatuses.size());
        assertEquals(Lists.newArrayList(baseBucketFS, deltaBucketFS), fileStatuses);
        verify(fs, times(1)).listStatus(tableRootPath);
        verify(fs, times(1)).listStatus(c310Path);
        verify(fs, times(1)).listStatus(basePath);
        verify(fs, times(1)).listStatus(deltaPath);
        verify(fs, never()).listStatus(deleteDeltaPath);
    }

    @Test
    public void testMatchPartitions()
    {
        FileStatus status;
        String[] partitions;

        status = getMockFileStatus("/path/to/file");
        partitions = new String[]{"p=1"};
        assertFalse(HadoopUtil.matchPartitions(status, partitions));

        status = getMockFileStatus("/path/to/file/p=1/foo.txt");
        partitions = new String[]{"p=1"};
        assertTrue(HadoopUtil.matchPartitions(status, partitions));

        status = getMockFileStatus("/path/to/file/p=1/q=x/foo.txt");
        partitions = new String[]{"p=1", "q=x"};
        assertTrue(HadoopUtil.matchPartitions(status, partitions));

        status = getMockFileStatus("/path/to/file/p=1/q=x/foo.txt");
        partitions = new String[]{"q=x"};
        assertTrue(HadoopUtil.matchPartitions(status, partitions));

        status = getMockFileStatus("/path/to/file/p=1/foo.txt");
        partitions = new String[]{"p=1", "q=x"};
        assertTrue(HadoopUtil.matchPartitions(status, partitions));
    }

    private FileStatus getMockFileStatus(String filepath)
    {
        Path path = mock(Path.class);
        when(path.toString()).thenReturn(filepath);

        FileStatus status = mock(FileStatus.class);
        when(status.getPath()).thenReturn(path);

        return status;
    }

    @AfterTest
    public void cleanUp()
    {
        synchronized (UserGroupInformation.class) {
            UserGroupInformation.reset();
            assertFalse(UserGroupInformation.isSecurityEnabled(),
                    "Kerberos auth is not set back to simple, this may cause other unit tests to fail");
        }
    }
}
