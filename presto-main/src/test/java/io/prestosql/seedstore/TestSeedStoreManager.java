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
package io.prestosql.seedstore;

import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.seedstore.SeedStoreFactory;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for SeedStoreManager
 *
 * @since 2020-03-25
 */
@Test(singleThreaded = true)
public class TestSeedStoreManager
{
    private SeedStoreManager seedStoreManager;
    private SeedStoreFactory factory;

    @BeforeTest
    private void prepareConfigFiles()
            throws IOException
    {
        File seedStoreConfigFile = new File("etc/seed-store.properties");
        if (!seedStoreConfigFile.exists()) {
            seedStoreConfigFile.createNewFile();
        }
        else {
            seedStoreConfigFile.delete();
            seedStoreConfigFile.createNewFile();
        }
        FileWriter configWritter = new FileWriter("etc/seed-store.properties");
        configWritter.write("seed-store.type=filebased\n");
        // Any profile, must be provided but will not be used
        configWritter.write("seed-store.filesystem.profile=etc/filesystem/hdfs-config-default.properties\n");
        // Set heartbeat to 1 seconds
        configWritter.write("seed-store.seed.heartbeat=1000\n");
        // Set heartbeat timeout to 3 seconds
        configWritter.write("seed-store.seed.heartbeat.timeout=3000");
        configWritter.close();
    }

    @BeforeMethod
    private void setUp()
            throws IOException
    {
        FileSystemClientManager mockFileSystemClientManager = mock(FileSystemClientManager.class);
        when(mockFileSystemClientManager.getFileSystemClient(anyString(), any())).thenReturn(null);
        seedStoreManager = new SeedStoreManager(mockFileSystemClientManager);
        SeedStore mockSeedStore = new MockSeedStore();

        mockSeedStore.add(new HashSet<>());
        SeedStoreFactory mockSeedStoreFactory = mock(SeedStoreFactory.class);
        when(mockSeedStoreFactory.getName()).thenReturn("filebased");
        when(mockSeedStoreFactory.create(any(String.class),
                any(HetuFileSystemClient.class),
                any(Map.class))).thenReturn(mockSeedStore);
        seedStoreManager.addSeedStoreFactory(mockSeedStoreFactory);
    }

    @Test
    public void testLoadAndGetSeedStore()
            throws IOException
    {
        seedStoreManager.loadSeedStore();
        SeedStore returned = seedStoreManager.getSeedStore();
    }

    @Test
    public void testAddToSeedStore()
            throws Exception
    {
        String location1 = "location1";
        String location2 = "location2";
        seedStoreManager.loadSeedStore();
        seedStoreManager.addSeed(location1, false);
        seedStoreManager.addSeed(location2, true);
        Collection<Seed> result = seedStoreManager.getAllSeeds();
        Assert.assertEquals(result.size(), 2);
        Assert.assertTrue(result.stream().filter(s -> s.getLocation().equals(location1)).findAny().isPresent());
        Assert.assertTrue(result.stream().filter(s -> s.getLocation().equals(location2)).findAny().isPresent());

        long timestamp1Old = result.stream().filter(s -> s.getLocation().equals(location1)).findAny().get().getTimestamp();
        long timestamp2Old = result.stream().filter(s -> s.getLocation().equals(location2)).findAny().get().getTimestamp();
        //wait 2 seconds, seed2 will be updated with new timestamp
        Thread.sleep(2000);
        result = seedStoreManager.getAllSeeds();
        long timestamp1New = result.stream().filter(s -> s.getLocation().equals(location1)).findAny().get().getTimestamp();
        long timestamp2New = result.stream().filter(s -> s.getLocation().equals(location2)).findAny().get().getTimestamp();
        Assert.assertTrue(timestamp1Old == timestamp1New);
        Assert.assertTrue(timestamp2Old < timestamp2New);
    }

    @Test
    void testRemoveFromSeedStore()
            throws Exception
    {
        String location1 = "location1";
        String location2 = "location2";
        String location3 = "location3";
        seedStoreManager.loadSeedStore();
        seedStoreManager.addSeed(location1, false);
        seedStoreManager.addSeed(location2, true);
        Assert.assertEquals(seedStoreManager.getAllSeeds().size(), 2);
        seedStoreManager.removeSeed(location1);
        seedStoreManager.removeSeed(location3);
        Collection<Seed> result = seedStoreManager.getAllSeeds();
        Assert.assertEquals(result.size(), 1);
        Assert.assertFalse(result.stream().filter(s -> s.getLocation().equals(location1)).findAny().isPresent());
    }

    @Test
    void testClearExpiredSeed()
            throws Exception
    {
        String location1 = "location1";
        String location2 = "location2";
        seedStoreManager.loadSeedStore();
        seedStoreManager.addSeed(location1, false);
        seedStoreManager.addSeed(location2, true);
        Assert.assertEquals(seedStoreManager.getAllSeeds().size(), 2);
        //wait 5 seconds, location1 expired since refreshable is not enabled
        Thread.sleep(5000);
        seedStoreManager.clearExpiredSeeds();
        Collection<Seed> result = seedStoreManager.getAllSeeds();
        Assert.assertEquals(result.size(), 1);
        Assert.assertFalse(result.stream().filter(s -> s.getLocation().equals(location1)).findAny().isPresent());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    void testDupAddFactory()
    {
        SeedStoreFactory mockSeedStoreFactory2 = mock(SeedStoreFactory.class);
        when(mockSeedStoreFactory2.getName()).thenReturn("filebased");
        seedStoreManager.addSeedStoreFactory(mockSeedStoreFactory2);
    }

    class MockSeed
            implements Seed
    {
        private static final long serialVersionUID = 4L;

        String location;
        long timestamp;

        /**
         * Constructor for the mock seed
         *
         * @param location host location of this seed
         */
        public MockSeed(String location, long timestamp)
        {
            this.location = location;
            this.timestamp = timestamp;
        }

        @Override
        public String getLocation()
        {
            return location;
        }

        @Override
        public long getTimestamp()
        {
            return timestamp;
        }

        @Override
        public String serialize()
                throws IOException
        {
            return "MOCK SEED. SHOULD NOT SERIALIZE.";
        }

        public void setTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            MockSeed mockSeed = (MockSeed) obj;
            return location.equals(mockSeed.location);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(location);
        }

        @Override
        public String toString()
        {
            return "MockSeed{"
                    + "location='" + location + '\''
                    + ", timestamp=" + timestamp + '}';
        }
    }

    class MockSeedStore
            implements SeedStore
    {
        private static final int INITIAL_SIZE = 0;
        private Set<Seed> seeds;

        /**
         * Constructor for the mock seed store
         */
        public MockSeedStore()
        {
            this.seeds = new HashSet<>(INITIAL_SIZE);
        }

        @Override
        public Collection<Seed> add(Collection<Seed> seedsToAdd)
        {
            // overwrite all seeds
            this.seeds.removeAll(seedsToAdd);
            this.seeds.addAll(seedsToAdd);
            return this.seeds;
        }

        @Override
        public Collection<Seed> get()
        {
            return seeds;
        }

        @Override
        public Collection<Seed> remove(Collection<Seed> seedsToRemove)
        {
            this.seeds.removeAll(seedsToRemove);
            return this.seeds;
        }

        @Override
        public Seed create(Map<String, String> properties)
        {
            String location = properties.get(Seed.LOCATION_PROPERTY_NAME);
            long timestamp = Long.parseLong(properties.get(Seed.TIMESTAMP_PROPERTY_NAME));
            return new MockSeed(location, timestamp);
        }

        @Override
        public String getName()
        {
            return "mock";
        }

        @Override
        public void setName(String name)
        {
        }
    }
}
