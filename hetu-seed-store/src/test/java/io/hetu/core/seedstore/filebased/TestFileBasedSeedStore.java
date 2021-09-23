/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.seedstore.filebased;

import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStoreSubType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * FilebasedSeedStore unit test
 *
 * @since 2020-03-04
 */
@Test(singleThreaded = true)
public class TestFileBasedSeedStore
{
    FileBasedSeedStoreFactory filebasedSeedStoreFactory;
    FileBasedSeedStore seedStore;
    private String clusterName = this.getClass().getSimpleName();
    static String rootDir = "/tmp/test/seedstore";

    /**
     * set up before unit test runs
     */
    @BeforeMethod
    public void setUp()
    {
        Map<String, String> config = new HashMap<>(0);
        config.put(FileBasedSeedConstants.SEED_STORE_FILESYSTEM_DIR, rootDir);
        filebasedSeedStoreFactory = new FileBasedSeedStoreFactory();
        seedStore = (FileBasedSeedStore) filebasedSeedStoreFactory.create("filebased", SeedStoreSubType.HAZELCAST,
                new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(rootDir)), config);
        seedStore.setName(clusterName);
    }

    /**
     * Tear down after unit test runs
     *
     * @throws IOException IOException happens if filesystem error happens
     */
    @AfterMethod
    public void tearDown()
    {
    }

    /**
     * Test add, remove and overwrite
     *
     * @throws IOException IOException happens if filesystem error happens
     */
    @Test
    public void testBasics()
            throws IOException
    {
        String ip1 = "10.0.0.1";
        final long timestamp1 = 1000L;
        String ip2 = "10.0.0.2";
        final long timestamp2 = 2000L;
        final int resultSize = 2;
        Seed seed1 = new FileBasedSeed(ip1, timestamp1);
        Seed seed2 = new FileBasedSeed(ip2, timestamp2);

        // add operation
        seedStore.add(Lists.newArrayList(seed1, seed2));
        Set<Seed> results = seedStore.get();
        assertEquals(results.size(), resultSize);
        assertTrue(results.contains(seed1));
        assertTrue(results.contains(seed2));
        Seed seedResult1 = getSeed(results, ip1);
        Seed seedResult2 = getSeed(results, ip2);
        assertEquals(seedResult1.getTimestamp(), timestamp1);
        assertEquals(seedResult2.getTimestamp(), timestamp2);

        // remove operation
        seedStore.remove(Lists.newArrayList(seed1));
        results = seedStore.get();
        assertEquals(results.size(), 1);
        assertFalse(results.contains(seed1));
        assertTrue(results.contains(seed2));
        seedResult2 = getSeed(results, ip2);
        assertEquals(seedResult2.getTimestamp(), timestamp2);

        // overwrite operation
        final long updateTimestamp2 = 3000L;
        Seed seed2Update = new FileBasedSeed(ip2, updateTimestamp2);
        seedStore.add(Lists.newArrayList(seed2Update));
        results = seedStore.get();
        assertEquals(results.size(), 1);
        assertTrue(results.contains(seed2));
        seedResult2 = getSeed(results, ip2);
        assertEquals(seedResult2.getTimestamp(), updateTimestamp2);
    }

    /**
     * Test Seed Creation
     */
    @Test
    public void testSeedCreate()
    {
        Map<String, String> prop = new HashMap<>(0);
        String location = "120.0.0.1";
        String timestamp = "100";
        prop.put(Seed.LOCATION_PROPERTY_NAME, location);
        prop.put(Seed.TIMESTAMP_PROPERTY_NAME, timestamp);
        Seed seed = seedStore.create(prop);
        assertEquals(seed.getLocation(), location);
        assertEquals(String.valueOf(seed.getTimestamp()), timestamp);
    }

    private Seed getSeed(Set<Seed> seeds, String location)
    {
        return seeds.stream().filter(s -> s.getLocation().equals(location)).findAny().get();
    }
}
