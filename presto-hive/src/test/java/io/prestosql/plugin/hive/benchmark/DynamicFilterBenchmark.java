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
package io.prestosql.plugin.hive.benchmark;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSource;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class DynamicFilterBenchmark
{
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        private Map<ColumnHandle, DynamicFilter> dynamicFilters;
        private Page page;
        private List<HivePartitionKey> partitions;
        private Map<Integer, ColumnHandle> eligibleColumns = new HashMap<>();

        public BenchmarkData()
        {
            final int numValues = 1024;
            BlockBuilder builder = new LongArrayBlockBuilder(null, numValues);
            for (int i = 0; i < numValues; i++) {
                builder.writeLong(i);
            }
            page = new Page(builder.build(), builder.build());

            dynamicFilters = new HashMap<>();
            ColumnHandle dayColumn = new HiveColumnHandle("pt_d", HIVE_INT, parseTypeSignature(INTEGER), 0, REGULAR, Optional.empty());
            ColumnHandle appColumn = new HiveColumnHandle("app_d", HIVE_INT, parseTypeSignature(INTEGER), 1, PARTITION_KEY, Optional.empty());

            BloomFilter dayFilter = new BloomFilter(1024 * 1024, 0.01);
            for (int i = 0; i < 10; i++) {
                dayFilter.add(i);
            }
            BloomFilter appFilter = new BloomFilter(1024 * 1024, 0.01);
            for (int i = 1023; i > 1013; i--) {
                appFilter.add(i);
            }

            dynamicFilters.put(dayColumn, new BloomFilterDynamicFilter("1", dayColumn, dayFilter, DynamicFilter.Type.GLOBAL));
            dynamicFilters.put(appColumn, new BloomFilterDynamicFilter("2", appColumn, appFilter, DynamicFilter.Type.GLOBAL));

            eligibleColumns.put(0, dayColumn);
            eligibleColumns.put(1, appColumn);

            partitions = new ArrayList<>();
            partitions.add(new HivePartitionKey("app_id", "10000"));
        }

        public Map<ColumnHandle, DynamicFilter> getDynamicFilters()
        {
            return dynamicFilters;
        }

        public Page getPage()
        {
            return page;
        }

        public List<HivePartitionKey> getPartitions()
        {
            return partitions;
        }

        public List<Map<Integer, ColumnHandle>> getEligibleColumns()
        {
            return ImmutableList.of(eligibleColumns);
        }
    }

    @Benchmark
    public void testFilterRows(BenchmarkData data)
    {
        List<Map<ColumnHandle, DynamicFilter>> dynamicFilters = new ArrayList<>();
        dynamicFilters.add(data.getDynamicFilters());
        Page filteredPage = HivePageSource.filter(dynamicFilters, data.getPage(), data.getEligibleColumns(), new Type[] {BIGINT, BIGINT});
    }

    @Benchmark
    public void testIsPartitionFiltered(BenchmarkData data)
    {
        isPartitionFiltered(data.getPartitions(), ImmutableList.of(new HashSet<>(data.getDynamicFilters().values())), null);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + io.prestosql.plugin.hive.benchmark.DynamicFilterBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
