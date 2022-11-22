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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.Int128ArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.ScalarOperator;

import java.util.UUID;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.prestosql.spi.block.Int128ArrayBlock.INT128_BYTES;
import static io.prestosql.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Long.reverseBytes;
import static java.lang.String.format;

public class IcebergUuidType
        extends AbstractType
        implements FixedWidthType
{
    public static final IcebergUuidType UUID = new IcebergUuidType();

    private IcebergUuidType()
    {
        super(parseTypeSignature(StandardTypes.UUID), Slice.class);
    }

    @Override
    public int getFixedSize()
    {
        return INT128_BYTES;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new Int128ArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Int128ArrayBlockBuilder(null, positionCount);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.getLong(leftPosition, SIZE_OF_LONG) == rightBlock.getLong(rightPosition, SIZE_OF_LONG) &&
                leftBlock.getLong(leftPosition, 0) == rightBlock.getLong(rightPosition, 0);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // compare high order bits
        int compare = Long.compare(leftBlock.getLong(leftPosition, SIZE_OF_LONG), rightBlock.getLong(rightPosition, SIZE_OF_LONG));
        if (compare != 0) {
            return compare;
        }
        // compare low order bits
        return Long.compare(leftBlock.getLong(leftPosition, 0), rightBlock.getLong(rightPosition, 0));
    }

    @Override
    public long hash(Block block, int position)
    {
        return XxHash64.hash(getSlice(block, position));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new UUID(block.getLong(position, 0), block.getLong(position, SIZE_OF_LONG)).toString();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0));
            blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (length != INT128_BYTES) {
            throw new IllegalStateException("Expected entry size to be exactly " + INT128_BYTES + " but was " + length);
        }
        blockBuilder.writeLong(value.getLong(offset));
        blockBuilder.writeLong(value.getLong(offset + SIZE_OF_LONG));
        blockBuilder.closeEntry();
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return Slices.wrappedLongArray(
                block.getLong(position, 0),
                block.getLong(position, SIZE_OF_LONG));
    }

    public static UUID trinoUuidToJavaUuid(Slice uuid)
    {
        if (uuid.length() != INT128_BYTES) {
            throw new IllegalStateException(format("Expected value to be exactly %d bytes but was %d", INT128_BYTES, uuid.length()));
        }
        return new UUID(
                reverseBytes(uuid.getLong(0)),
                reverseBytes(uuid.getLong(SIZE_OF_LONG)));
    }

    public static Slice javaUuidToTrinoUuid(UUID uuid)
    {
        return Slices.wrappedLongArray(
                reverseBytes(uuid.getMostSignificantBits()),
                reverseBytes(uuid.getLeastSignificantBits()));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(Slice left, Slice right)
    {
        return compareLittleEndian(
                left.getLong(0),
                left.getLong(SIZE_OF_LONG),
                right.getLong(0),
                right.getLong(SIZE_OF_LONG));
    }

    private static int compareLittleEndian(long leftLow64le, long leftHigh64le, long rightLow64le, long rightHigh64le)
    {
        int compare = Long.compareUnsigned(reverseBytes(leftLow64le), reverseBytes(rightLow64le));
        if (compare != 0) {
            return compare;
        }
        return Long.compareUnsigned(reverseBytes(leftHigh64le), reverseBytes(rightHigh64le));
    }
}
