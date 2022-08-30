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
package io.hetu.core.plugin.exchange.filesystem.checksum;

import io.prestosql.spi.checksum.Checksum;
import io.prestosql.spi.exchange.checksum.ExchangeMarkerChecksumFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.prestosql.spi.checksum.CheckSumAlgorithm.MD5;
import static io.prestosql.spi.checksum.CheckSumAlgorithm.MURMUR3;
import static io.prestosql.spi.checksum.CheckSumAlgorithm.SHA256;
import static io.prestosql.spi.checksum.CheckSumAlgorithm.SHA512;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test the hash algorithms (MD5, SHA, MURMUR3) for Performance over a million randomly generated UUIDs.
 */
public class CheckSumAlgorithmPerformanceTest
{
    private static final ExchangeMarkerChecksumFactory checksumFactory = new ExchangeMarkerChecksumFactory();
    private static final int MAX_ELEMENTS = 1000 * 1000;
    private static final List<String> UUIDs = new ArrayList<>(MAX_ELEMENTS);

    @BeforeClass
    public static void onlyOnce()
    {
        for (int index = 0; index < MAX_ELEMENTS; index++) {
            UUIDs.add(UUID.randomUUID().toString());
        }
    }

    @Test
    public void testMD5()
    {
        long start = System.currentTimeMillis();

        Checksum checksum = checksumFactory.create(MD5);
        for (String uuid : UUIDs) {
            byte[] bytes = uuid.getBytes(UTF_8);
            checksum.update(bytes);
        }

        long end = System.currentTimeMillis();
        System.out.println("Time taken to compute MD5 hash: " + (end - start) + "ms.");
    }

    @Test
    public void testSHA256()
    {
        long start = System.currentTimeMillis();

        Checksum checksum = checksumFactory.create(SHA256);
        for (String uuid : UUIDs) {
            byte[] bytes = uuid.getBytes(UTF_8);
            checksum.update(bytes);
        }

        long end = System.currentTimeMillis();
        System.out.println("Time taken to compute SHA-256 hash: " + (end - start) + "ms.");
    }

    @Test
    public void testSHA512()
    {
        long start = System.currentTimeMillis();

        Checksum checksum = checksumFactory.create(SHA512);
        for (String uuid : UUIDs) {
            byte[] bytes = uuid.getBytes(UTF_8);
            checksum.update(bytes);
        }

        long end = System.currentTimeMillis();
        System.out.println("Time taken to compute SHA-512 hash: " + (end - start) + "ms.");
    }

    @Test
    public void testMurmur3()
    {
        long start = System.currentTimeMillis();

        Checksum checksum = checksumFactory.create(MURMUR3);
        for (String uuid : UUIDs) {
            byte[] bytes = uuid.getBytes(UTF_8);
            checksum.update(bytes);
        }

        long end = System.currentTimeMillis();
        System.out.println("Time taken to compute MURMUR3 hash: " + (end - start) + "ms.");
    }
}
