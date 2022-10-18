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
import io.airlift.slice.XxHash64;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.IntArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.function.ScalarOperator;

import static io.prestosql.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Long.rotateLeft;
import static java.lang.invoke.MethodHandles.lookup;

public abstract class AbstractIntType
        extends AbstractType
        implements FixedWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(AbstractIntType.class, lookup(), long.class);

    protected AbstractIntType(TypeSignature signature)
    {
        super(signature, long.class);
    }

    @Override
    public final int getFixedSize()
    {
        return Integer.BYTES;
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
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public final long getLong(Block block, int position)
    {
        return block.getInt(position, 0);
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeInt((int) value).closeEntry();
    }

    @Override
    public final void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftValue = leftBlock.getInt(leftPosition, 0);
        int rightValue = rightBlock.getInt(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return hash(block.getInt(position, 0));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // WARNING: the correctness of InCodeGenerator is dependent on the implementation of this
        // function being the equivalence of internal long representation.
        int leftValue = leftBlock.getInt(leftPosition, 0);
        int rightValue = rightBlock.getInt(rightPosition, 0);
        return Integer.compare(leftValue, rightValue);
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new IntArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Integer.BYTES));
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Integer.BYTES);
    }

    @Override
    public final BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new IntArrayBlockBuilder(null, positionCount);
    }

    public static long hash(int value)
    {
        // xxhash64 mix
        return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long left, long right)
    {
        return left == right;
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        return AbstractLongType.hash((int) value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        return XxHash64.hash((int) value);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(long left, long right)
    {
        return Integer.compare((int) left, (int) right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return ((int) left) < ((int) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return ((int) left) <= ((int) right);
    }
}
