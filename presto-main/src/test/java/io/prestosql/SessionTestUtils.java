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
package io.prestosql;

import io.prestosql.client.ClientCapabilities;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.snapshot.SnapshotConfig;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.utils.HetuConfig;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.SystemSessionProperties.REUSE_TABLE_SCAN;
import static io.prestosql.SystemSessionProperties.SNAPSHOT_ENABLED;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.stream;

public final class SessionTestUtils
{
    public static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema(TINY_SCHEMA_NAME)
            .setClientCapabilities(stream(ClientCapabilities.values())
                    .map(ClientCapabilities::toString)
                    .collect(toImmutableSet()))
            .build();

    public static final Session TEST_SESSION_REUSE = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema(TINY_SCHEMA_NAME)
            .setClientCapabilities(stream(ClientCapabilities.values())
                    .map(ClientCapabilities::toString)
                    .collect(toImmutableSet()))
            .setSystemProperty(REUSE_TABLE_SCAN, "true")
            .build();

    public static final Session TEST_SNAPSHOT_SESSION;

    static
    {
        // Use a session object that enables snapshot
        SnapshotConfig snapshotConfig = new SnapshotConfig();
        SystemSessionProperties properties = new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                new HetuConfig(),
                snapshotConfig);
        TEST_SNAPSHOT_SESSION = testSessionBuilder(new SessionPropertyManager(properties))
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .setSystemProperty(SNAPSHOT_ENABLED, "true")
                .build();
    }

    private SessionTestUtils()
    {
    }
}
