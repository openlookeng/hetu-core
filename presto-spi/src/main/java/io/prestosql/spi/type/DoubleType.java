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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.ScalarOperator;

import java.util.Optional;

import static io.prestosql.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.prestosql.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.invoke.MethodHandles.lookup;

public final class DoubleType
        extends AbstractType
        implements FixedWidthType
{
    public static final DoubleType DOUBLE = new DoubleType();

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(DoubleType.class, lookup(), double.class);

    private DoubleType()
    {
        super(parseTypeSignature(StandardTypes.DOUBLE), double.class);
    }

    @Override
    public final int getFixedSize()
    {
        return Double.BYTES;
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return longBitsToDouble(block.getLong(position, 0));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        double leftValue = longBitsToDouble(leftBlock.getLong(leftPosition, 0));
        double rightValue = longBitsToDouble(rightBlock.getLong(rightPosition, 0));

        // direct equality is correct here
        // noinspection FloatingPointEquality
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        // convert to canonical NaN if necessary
        return AbstractLongType.hash(doubleToLongBits(longBitsToDouble(block.getLong(position, 0))));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        double leftValue = longBitsToDouble(leftBlock.getLong(leftPosition, 0));
        double rightValue = longBitsToDouble(rightBlock.getLong(rightPosition, 0));
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0)).closeEntry();
        }
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return longBitsToDouble(block.getLong(position, 0));
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        blockBuilder.writeLong(doubleToLongBits(value)).closeEntry();
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
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Double.BYTES));
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Double.BYTES);
    }

    @Override
    public final BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == DOUBLE;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public Optional<Range> getRange()
    {
        // The range for double is undefined because NaN is a special value that
        // is *not* in any reasonable definition of a range for this type.
        return Optional.empty();
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonUnorderedLastOperator(double left, double right)
    {
        return Double.compare(left, right);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }
}
