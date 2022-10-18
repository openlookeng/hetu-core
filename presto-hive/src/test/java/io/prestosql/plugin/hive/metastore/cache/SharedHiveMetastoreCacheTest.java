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
package io.prestosql.plugin.hive.metastore.cache;

import io.prestosql.plugin.hive.metastore.HiveMetastoreFactory;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.testing.TestingNodeManager;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.mockito.MockitoAnnotations.initMocks;

public class SharedHiveMetastoreCacheTest
{
    @Mock
    private NodeManager mockNodeManager;
    @Mock
    private CachingHiveMetastoreConfig mockConfig;
    @Mock
    private ImpersonationCachingConfig mockImpersonationCachingConfig;

    private SharedHiveMetastoreCache sharedHiveMetastoreCacheUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        sharedHiveMetastoreCacheUnderTest = new SharedHiveMetastoreCache(
                new CatalogName("catalogName"),
                new TestingNodeManager(),
                new CachingHiveMetastoreConfig(),
                new ImpersonationCachingConfig());
    }

    @Test
    public void testStart() throws Exception
    {
        // Setup
        // Run the test
        sharedHiveMetastoreCacheUnderTest.start();

        // Verify the results
    }

    @Test
    public void testStop()
    {
        // Setup
        // Run the test
        sharedHiveMetastoreCacheUnderTest.stop();

        // Verify the results
    }

    @Test
    public void testIsEnabled()
    {
        boolean enabled = sharedHiveMetastoreCacheUnderTest.isEnabled();
    }

    @Test
    public void testCreateCachingHiveMetastoreFactory()
    {
        // Setup
        final HiveMetastoreFactory metastoreFactory = HiveMetastoreFactory.ofInstance(FileHiveMetastore.createTestingFileHiveMetastore(new File("file")));

        // Run the test
        final HiveMetastoreFactory result = sharedHiveMetastoreCacheUnderTest.createCachingHiveMetastoreFactory(
                metastoreFactory);

        // Verify the results
    }
}
