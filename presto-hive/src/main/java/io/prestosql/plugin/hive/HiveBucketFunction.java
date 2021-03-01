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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.BucketFunction;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveBucketFunction
        implements BucketFunction
{
    private final BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final List<TypeInfo> typeInfos;
    private final List<TypeInfo> typeInfosForUpdate;
    private final boolean isRowIdPartitioner;

    public HiveBucketFunction(BucketingVersion bucketingVersion, int bucketCount, List<HiveType> hiveTypes)
    {
        this(bucketingVersion, bucketCount, hiveTypes, false);
    }

    public HiveBucketFunction(BucketingVersion bucketingVersion, int bucketCount, List<HiveType> hiveTypes, boolean forUpdate)
    {
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.typeInfos = requireNonNull(hiveTypes, "hiveTypes is null").stream()
                .map(HiveType::getTypeInfo)
                .collect(Collectors.toList());
        this.isRowIdPartitioner = forUpdate &&
                typeInfos.get(typeInfos.size() - 1).getCategory() == Category.STRUCT;
        if (forUpdate && typeInfos.size() > 1) {
            typeInfosForUpdate = typeInfos.subList(0, typeInfos.size() - 1);
        }
        else {
            typeInfosForUpdate = ImmutableList.of();
        }
    }

    @Override
    public int getBucket(Page page, int position)
    {
        if (isRowIdPartitioner) {
            int bucketHashCode = 0;
            if (page.getChannelCount() > 1) {
                //Consider the partitioning columns also for partitioning during update to parallelize the updates of partitioned tables.
                bucketHashCode = HiveBucketing.getBucketHashCode(bucketingVersion, typeInfosForUpdate, page, position, typeInfosForUpdate.size());
            }
            bucketHashCode = bucketHashCode * 31 + HiveBucketing.extractBucketNumber(page, position);
            return HiveBucketing.getBucketNumber(bucketHashCode, bucketCount);
        }
        return HiveBucketing.getHiveBucket(bucketingVersion, bucketCount, typeInfos, page, position);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", bucketingVersion)
                .add("bucketCount", bucketCount)
                .add("typeInfos", typeInfos)
                .toString();
    }
}
