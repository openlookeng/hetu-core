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
package io.prestosql.heuristicindex;

import io.hetu.core.heuristicindex.IndexClient;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.utils.HetuConstant;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestLocalIndexCacheLoader
{
    @BeforeTest
    private void setProperties()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.FILTER_PLUGINS, "");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_TYPE, HetuConstant.INDEXSTORE_TYPE_LOCAL);
        PropertyService.setProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE, 10L);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoLastModifiedTime() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        LocalIndexCacheLoader localIndexCacheLoader = new LocalIndexCacheLoader(indexclient);

        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", 1);

        // throw exception to produce "no last modified time file found" behaviour
        when(indexclient.getLastModified((indexCacheKey.getPath()))).thenThrow(Exception.class);

        localIndexCacheLoader.load(indexCacheKey);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoMatchingLastModifiedTime() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        LocalIndexCacheLoader localIndexCacheLoader = new LocalIndexCacheLoader(indexclient);

        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", 1L);

        // return different last modified time to simulate expired index
        when(indexclient.getLastModified((indexCacheKey.getPath()))).thenReturn(2L);

        localIndexCacheLoader.load(indexCacheKey);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoValidIndexFilesFoundException() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        LocalIndexCacheLoader localIndexCacheLoader = new LocalIndexCacheLoader(indexclient);

        long lastModifiedTime = 1L;
        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", lastModifiedTime);
        when(indexclient.getLastModified((indexCacheKey.getPath()))).thenReturn(lastModifiedTime);
        when(indexclient.readSplitIndex((indexCacheKey.getPath()))).thenThrow(Exception.class);

        localIndexCacheLoader.load(indexCacheKey);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoValidIndexFilesFound() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        LocalIndexCacheLoader localIndexCacheLoader = new LocalIndexCacheLoader(indexclient);

        long lastModifiedTime = 1L;
        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", lastModifiedTime);
        when(indexclient.getLastModified((indexCacheKey.getPath()))).thenReturn(lastModifiedTime);
        when(indexclient.readSplitIndex((indexCacheKey.getPath()))).thenReturn(Collections.emptyList());

        localIndexCacheLoader.load(indexCacheKey);
    }

    @Test
    public void testIndexFound() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        LocalIndexCacheLoader localIndexCacheLoader = new LocalIndexCacheLoader(indexclient);

        List<SplitIndexMetadata> expectedSplitIndexes = new LinkedList<>();
        expectedSplitIndexes.add(mock(SplitIndexMetadata.class));

        long lastModifiedTime = 1L;
        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", lastModifiedTime);
        when(indexclient.getLastModified((indexCacheKey.getPath()))).thenReturn(lastModifiedTime);
        when(indexclient.readSplitIndex((indexCacheKey.getPath()))).thenReturn(expectedSplitIndexes);

        List<SplitIndexMetadata> actualSplitIndexes = localIndexCacheLoader.load(indexCacheKey);
        assertEquals(expectedSplitIndexes.size(), actualSplitIndexes.size());
    }

    @Test
    public void testGetIndexStoreProperties()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.INDEXSTORE_URI, "/tmp/hetu/indices");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_TYPE, "local");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_CONFIG_RESOURCES, "/tmp/core-site.xml,/tmp/hdfs-site.xml");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_AUTHENTICATION_TYPE, "KERBEROS");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_CONFIG_PATH, "/tmp/krb5.conf");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_KEYTAB_PATH, "/tmp/user.keytab");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_PRINCIPAL, "user");
        // Filter Plugin is needed to instantiate LocalIndexCacheLoader.indexClient
        // but it won't be part of the expected properties because only the IndexFactory needs to know the plugin dir
        // IndexClient should be ignorant to the plugin directory
        PropertyService.setProperty(HetuConstant.FILTER_PLUGINS, "");

        Properties actual = LocalIndexCacheLoader.getIndexStoreProperties();

        Properties expected = new Properties();
        expected.setProperty("uri", "/tmp/hetu/indices");
        expected.setProperty("type", "local");
        expected.setProperty("hdfs.config.resources", "/tmp/core-site.xml,/tmp/hdfs-site.xml");
        expected.setProperty("hdfs.authentication.type", "KERBEROS");
        expected.setProperty("hdfs.krb5.conf.path", "/tmp/krb5.conf");
        expected.setProperty("hdfs.krb5.keytab.path", "/tmp/user.keytab");
        expected.setProperty("hdfs.krb5.principal", "user");

        assertEquals(actual, expected);
    }
}
