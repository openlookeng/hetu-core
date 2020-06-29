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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
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
        configWritter.write("seed-store.type=hdfs\n");
        // Any profile, must be provided but will not be used
        configWritter.write("seed-store.filesystem.profile=etc/filesystem/hdfs-config-default.properties");
        configWritter.close();
    }

    @BeforeMethod
    private void setUp()
            throws IOException
    {
        FileSystemClientManager mockFileSystemClientManager = mock(FileSystemClientManager.class);
        when(mockFileSystemClientManager.getFileSystemClient(anyString())).thenReturn(null);
        seedStoreManager = new SeedStoreManager(mockFileSystemClientManager);
        SeedStore mockSeedStore = new MockSeedStore();

        HashSet<Seed> seeds = new HashSet<>();
        seeds.add(new MockSeed("location1"));
        seeds.add(new MockSeed("location2"));
        mockSeedStore.add(seeds);
        SeedStoreFactory mockSeedStoreFactory = mock(SeedStoreFactory.class);
        when(mockSeedStoreFactory.getName()).thenReturn("hdfs");
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
        seedStoreManager.loadSeedStore();
        seedStoreManager.addSeedToSeedStore("location3");
    }

    @Test
    void testRemoveFromSeedStore()
            throws Exception
    {
        seedStoreManager.loadSeedStore();
        seedStoreManager.removeSeedFromSeedStore("location1");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    void testDupAddFactory()
    {
        SeedStoreFactory mockSeedStoreFactory2 = mock(SeedStoreFactory.class);
        when(mockSeedStoreFactory2.getName()).thenReturn("hdfs");
        seedStoreManager.addSeedStoreFactory(mockSeedStoreFactory2);
    }

    class MockSeed
            implements Seed
    {
        private static final long serialVersionUID = 4L;

        String location;
        long timeStamp;

        /**
         * Constructor for the mock seed
         *
         * @param location host location of this seed
         */
        public MockSeed(String location)
        {
            this.location = location;
            timeStamp = 0L;
        }

        @Override
        public String getLocation()
        {
            return location;
        }

        @Override
        public long getTimestamp()
        {
            return 0L;
        }

        @Override
        public String serialize()
                throws IOException
        {
            return "MOCK SEED. SHOULD NOT SERIALIZE.";
        }

        public void setTimeStamp(long timeStamp)
        {
            this.timeStamp = timeStamp;
        }
    }

    class MockSeedStore
            implements SeedStore
    {
        private static final int INITIAL_SIZE = 2;
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
            return new MockSeed(location);
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
