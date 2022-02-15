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
package io.prestosql.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.resourcegroups.ResourceGroupNameTemplate;
import io.prestosql.plugin.resourcegroups.SelectorResourceEstimate;
import io.prestosql.plugin.resourcegroups.SelectorResourceEstimate.Range;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.prestosql.spi.resourcegroups.QueryType.DELETE;
import static io.prestosql.spi.resourcegroups.QueryType.EXPLAIN;
import static io.prestosql.spi.resourcegroups.QueryType.INSERT;
import static io.prestosql.spi.resourcegroups.QueryType.SELECT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestResourceGroupsDao
{
    private static final String ENVIRONMENT = "test";
    private static final SelectorResourceEstimate SELECTOR_RESOURCE_ESTIMATE = new SelectorResourceEstimate(
            Optional.of(new Range<>(
                    Optional.empty(),
                    Optional.of(Duration.valueOf("5m")))),
            Optional.of(new SelectorResourceEstimate.Range<>(
                    Optional.of(Duration.valueOf("10s")),
                    Optional.empty())),
            Optional.of(new Range<>(
                    Optional.empty(),
                    Optional.of(DataSize.valueOf("500MB")))));

    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);
    private static final JsonCodec<SelectorResourceEstimate> SELECTOR_RESOURCE_ESTIMATE_JSON_CODEC = jsonCodec(SelectorResourceEstimate.class);

    private static final Pattern PING_USER = Pattern.compile("ping_user");
    private static final Pattern ANY = Pattern.compile(".*");
    private static final Pattern ADMIN_USER = Pattern.compile("admin_user");
    private static final Pattern PING = Pattern.compile("ping.*");
    private static final Pattern PING_SOURCE = Pattern.compile("ping_source");
    private static final Pattern USER1 = Pattern.compile("user1");
    private static final Pattern PIPELINE = Pattern.compile("pipeline");

    static H2ResourceGroupsDao setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime() + ThreadLocalRandom.current().nextLong());
        return new H2DaoProvider(config).get();
    }

    @Test
    public void testResourceGroups()
    {
        H2ResourceGroupsDao dao = setup("resource_groups");
        dao.createResourceGroupsTable();
        Map<Long, ResourceGroupSpecBuilder> map = new HashMap<>();
        testResourceGroupInsert(dao, map);
        testResourceGroupUpdate(dao, map);
        testResourceGroupDelete(dao, map);
    }

    private static void testResourceGroupInsert(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        // Hetu: add values for new parameters softReservedMemory and hardReservedConcurrency
        dao.insertResourceGroup(1, "global", "100%", "10%", 100, 100, 100, 50, null, null, null, null, null, "RECENT_QUERIES", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "bi", "50%", "10%", 50, 50, 50, 50, null, null, null, null, null, "RECENT_QUERIES", 1L, ENVIRONMENT);
        List<ResourceGroupSpecBuilder> records = dao.getResourceGroups(ENVIRONMENT);
        assertEquals(records.size(), 2);
        map.put(1L, new ResourceGroupSpecBuilder(1, new ResourceGroupNameTemplate("global"), "100%", "10%", 100, Optional.of(100), 100, Optional.of(50), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("RECENT_QUERIES"), null));
        map.put(2L, new ResourceGroupSpecBuilder(2, new ResourceGroupNameTemplate("bi"), "50%", "10%", 50, Optional.of(50), 50, Optional.of(50), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("RECENT_QUERIES"), Optional.of(1L)));
        compareResourceGroups(map, records);
    }

    private static void testResourceGroupUpdate(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        // Hetu: add values for new parameters softReservedMemory and hardReservedConcurrency
        dao.updateResourceGroup(2, "bi", "40%", "10%", 40, 30, 50, 50, null, null, true, null, null, "RECENT_QUERIES", 1L, ENVIRONMENT);
        ResourceGroupSpecBuilder updated = new ResourceGroupSpecBuilder(2, new ResourceGroupNameTemplate("bi"), "40%", "10%", 40, Optional.of(30), 50, Optional.of(50), Optional.empty(), Optional.empty(), Optional.of(true), Optional.empty(), Optional.empty(), Optional.of("RECENT_QUERIES"), Optional.of(1L));
        map.put(2L, updated);
        compareResourceGroups(map, dao.getResourceGroups(ENVIRONMENT));
    }

    private static void testResourceGroupDelete(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.deleteResourceGroup(2);
        map.remove(2L);
        compareResourceGroups(map, dao.getResourceGroups(ENVIRONMENT));
    }

    @Test
    public void testSelectors()
    {
        H2ResourceGroupsDao dao = setup("selectors");
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        Map<Long, SelectorRecord> map = new HashMap<>();
        testSelectorInsert(dao, map);
        testSelectorUpdate(dao, map);
        testSelectorUpdateNull(dao, map);
        testSelectorDelete(dao, map);
        testSelectorDeleteNull(dao, map);
        testSelectorMultiDelete(dao, map);
    }

    private static void testSelectorInsert(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        map.put(2L,
                new SelectorRecord(
                        2L,
                        1L,
                        Optional.of(PING_USER),
                        Optional.of(ANY),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
        map.put(3L,
                new SelectorRecord(
                        3L,
                        2L,
                        Optional.of(ADMIN_USER),
                        Optional.of(ANY),
                        Optional.of(EXPLAIN.name()),
                        Optional.of(ImmutableList.of("tag1", "tag2")),
                        Optional.empty()));
        map.put(4L,
                new SelectorRecord(
                        4L,
                        0L,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(SELECTOR_RESOURCE_ESTIMATE)));
        // Hetu: add values for new parameters softReservedMemory and hardReservedConcurrency
        dao.insertResourceGroup(1, "admin", "100%", "10%", 100, 100, 100, 50, null, null, null, null, null, "RECENT_QUERIES", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "ping_query", "50%", "10%", 50, 50, 50, 50, null, null, null, null, null, "RECENT_QUERIES", 1L, ENVIRONMENT);
        dao.insertResourceGroup(3, "config", "50%", "10%", 50, 50, 50, 50, null, null, null, null, null, "RECENT_QUERIES", 1L, ENVIRONMENT);
        dao.insertResourceGroup(4, "config", "50%", "10%", 50, 50, 50, 50, null, null, null, null, null, "RECENT_QUERIES", 1L, ENVIRONMENT);

        dao.insertSelector(2, 1, "ping_user", ".*", null, null, null);
        dao.insertSelector(3, 2, "admin_user", ".*", EXPLAIN.name(), LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null);
        dao.insertSelector(4, 0, null, null, null, null, SELECTOR_RESOURCE_ESTIMATE_JSON_CODEC.toJson(SELECTOR_RESOURCE_ESTIMATE));
        List<SelectorRecord> records = dao.getSelectors(ENVIRONMENT);
        compareSelectors(map, records);
    }

    private static void testSelectorUpdate(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        dao.updateSelector(2, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1")), "ping_user", ".*", null);
        SelectorRecord updated = new SelectorRecord(
                2,
                1L,
                Optional.of(PING),
                Optional.of(PING_SOURCE),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1")),
                Optional.empty());
        map.put(2L, updated);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorUpdateNull(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        SelectorRecord updated = new SelectorRecord(2, 3L, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        map.put(2L, updated);
        dao.updateSelector(2, null, null, null, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1")));
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
        updated = new SelectorRecord(
                2,
                2L,
                Optional.of(PING),
                Optional.of(PING_SOURCE),
                Optional.of(EXPLAIN.name()),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty());
        map.put(2L, updated);
        dao.updateSelector(2, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null, null, null);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorDelete(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        map.remove(2L);
        dao.deleteSelector(2, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorDeleteNull(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        dao.updateSelector(3, null, null, null, "admin_user", ".*", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        SelectorRecord nullRegexes = new SelectorRecord(3L, 2L, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        map.put(3L, nullRegexes);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
        dao.deleteSelector(3, null, null, null);
        map.remove(3L);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorMultiDelete(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        if (dao != null) {
            return;
        }
        dao.insertSelector(3, 3L, "user1", "pipeline", null, null, null);
        map.put(3L, new SelectorRecord(
                3L,
                3L,
                Optional.of(USER1),
                Optional.of(PIPELINE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
        dao.deleteSelectors(3L);
        map.remove(3L);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    @Test
    public void testGlobalResourceGroupProperties()
    {
        H2ResourceGroupsDao dao = setup("global_properties");
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        ResourceGroupGlobalProperties globalProperties = new ResourceGroupGlobalProperties(Optional.of(Duration.valueOf("1h")));
        ResourceGroupGlobalProperties records = dao.getResourceGroupGlobalProperties().get(0);
        assertEquals(globalProperties, records);
        try {
            dao.insertResourceGroupsGlobalProperties("invalid_property", "1h");
        }
        catch (UnableToExecuteStatementException ex) {
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcException);
            assertTrue(ex.getCause().getMessage().startsWith("Check constraint violation:"));
        }
        try {
            dao.updateResourceGroupsGlobalProperties("invalid_property_name");
        }
        catch (UnableToExecuteStatementException ex) {
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcException);
            assertTrue(ex.getCause().getMessage().startsWith("Check constraint violation:"));
        }
    }

    @Test
    public void testExactMatchSelector()
    {
        H2ResourceGroupsDao dao = setup("exact_match_selector");
        dao.createExactMatchSelectorsTable();

        ResourceGroupId resourceGroupId1 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "insert"));
        ResourceGroupId resourceGroupId2 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "select"));
        JsonCodec<ResourceGroupId> codec = JsonCodec.jsonCodec(ResourceGroupId.class);
        dao.insertExactMatchSelector("test", "@test@test_pipeline", INSERT.name(), codec.toJson(resourceGroupId1));
        dao.insertExactMatchSelector("test", "@test@test_pipeline", SELECT.name(), codec.toJson(resourceGroupId2));

        assertNull(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", null));
        assertEquals(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", INSERT.name()), codec.toJson(resourceGroupId1));
        assertEquals(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", SELECT.name()), codec.toJson(resourceGroupId2));
        assertNull(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", DELETE.name()));

        assertNull(dao.getExactMatchResourceGroup("test", "abc", INSERT.name()));
        assertNull(dao.getExactMatchResourceGroup("prod", "@test@test_pipeline", INSERT.name()));
    }

    private static void compareResourceGroups(Map<Long, ResourceGroupSpecBuilder> map, List<ResourceGroupSpecBuilder> records)
    {
        assertEquals(map.size(), records.size());
        for (ResourceGroupSpecBuilder record : records) {
            ResourceGroupSpecBuilder expected = map.get(record.getId());
            assertEquals(record.build(), expected.build());
        }
    }

    private static void compareSelectors(Map<Long, SelectorRecord> map, List<SelectorRecord> records)
    {
        assertEquals(map.size(), records.size());
        for (SelectorRecord record : records) {
            SelectorRecord expected = map.get(record.getResourceGroupId());
            assertEquals(record.getResourceGroupId(), expected.getResourceGroupId());
            assertEquals(record.getUserRegex().map(Pattern::pattern), expected.getUserRegex().map(Pattern::pattern));
            assertEquals(record.getSourceRegex().map(Pattern::pattern), expected.getSourceRegex().map(Pattern::pattern));
            assertEquals(record.getSelectorResourceEstimate(), expected.getSelectorResourceEstimate());
        }
    }
}
