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
package io.hetu.core.eventlistener.listeners;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.hetu.core.eventlistener.HetuEventListenerPlugin;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingEventListenerManager;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryEventLogger
{
    private static final Path path = Paths.get("/tmp/hetu_listener.log");

    private final DistributedQueryRunner queryRunner;

    TestQueryEventLogger()
            throws Exception
    {
        Files.deleteIfExists(path);

        Session.SessionBuilder sessionBuilder = testSessionBuilder().setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        this.queryRunner = DistributedQueryRunner.builder(sessionBuilder.build()).setNodeCount(1).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.getCoordinator()
                .getInstance(Key.get(TestingEventListenerManager.class))
                .setProperties(ImmutableMap.of("hetu.event.listener.type", "LOGGER",
                        "hetu.event.listener.listen.query.creation", "true",
                        "hetu.event.listener.listen.query.completion", "true",
                        "hetu.event.listener.listen.split.completion", "true", "hetu.event.listener.logger.file",
                        path.toAbsolutePath().toString()));

        queryRunner.installPlugin(new HetuEventListenerPlugin());
    }

    @AfterClass
    public static void clean()
            throws IOException
    {
        Files.deleteIfExists(path);
    }

    @Test
    public void testHetuEventListener1()
    {
        MaterializedResult result = queryRunner.execute("show schemas");
        assertLog("Query Created", "show schemas", "Catalog: tpch", "Schema: tiny", "Split Completed");
    }

    @Test
    public void testHetuEventListener2()
    {
        try {
            queryRunner.execute("select * from tpch.tiny.fake_customer");
        }
        catch (RuntimeException ex) {
            // Query should fail but the listener should log the query
            ex.printStackTrace();
            assertLog("Query Created", "select * from tpch.tiny.fake_customer");
        }
    }

    private void assertLog(String... keywords)
    {
        try {
            String log = new String(Files.readAllBytes(path));
            System.out.println(log);
            for (String keyword : keywords) {
                assertTrue(log.contains(keyword), keyword + " not found in the log");
            }
            Files.write(path, new byte[0], StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
