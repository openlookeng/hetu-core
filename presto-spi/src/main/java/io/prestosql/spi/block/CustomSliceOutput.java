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

package io.prestosql.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public class CustomSliceOutput
        extends SliceOutput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(CustomSliceOutput.class).instanceSize();

    private Slice slice;

    private int size;

    public CustomSliceOutput(int estimatedSize)
    {
        this.slice = Slices.allocate(estimatedSize);
    }

    @Override
    public void reset()
    {
        size = 0;
    }

    @Override
    public void reset(int position)
    {
        checkArgument(position >= 0, "position is negative");
        checkArgument(position <= size, "position is larger than size");
        size = position;
    }

    public static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public long getRetainedSize()
    {
        return slice.getRetainedSize() + INSTANCE_SIZE;
    }

    @Override
    public boolean isWritable()
    {
        return writableBytes() > 0;
    }

    @Override
    public int writableBytes()
    {
        return slice.length() - size;
    }

    @Override
    public void writeByte(int value)
    {
        slice = Slices.ensureSize(slice, size + SIZE_OF_BYTE);
        slice.setByte(size, value);
        size += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int value)
    {
        slice = Slices.ensureSize(slice, size + SIZE_OF_SHORT);
        slice.setShort(size, value);
        size += SIZE_OF_SHORT;
    }

    @Override
    public void writeInt(int value)
    {
        slice = Slices.ensureSize(slice, size + SIZE_OF_INT);
        slice.setInt(size, value);
        size += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long value)
    {
        slice = Slices.ensureSize(slice, size + SIZE_OF_LONG);
        slice.setLong(size, value);
        size += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float value)
    {
        slice = Slices.ensureSize(slice, size + SIZE_OF_FLOAT);
        slice.setFloat(size, value);
        size += SIZE_OF_FLOAT;
    }

    @Override
    public void writeDouble(double value)
    {
        slice = Slices.ensureSize(slice, size + SIZE_OF_DOUBLE);
        slice.setDouble(size, value);
        size += SIZE_OF_DOUBLE;
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        slice = Slices.ensureSize(slice, size + length);
        slice.setBytes(size, source, sourceIndex, length);
        size += length;
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(Slice sourceSlice, int sourceIndex, int length)
    {
        slice = Slices.ensureSize(slice, size + length);
        if (slice.hasByteArray() && sourceSlice.hasByteArray()) {
            byte[] base = sourceSlice.byteArray();
            int byteArrayOffset = sourceSlice.byteArrayOffset();
            System.arraycopy(base, byteArrayOffset + sourceIndex, slice.byteArray(), size, length);
        }
        else {
            slice.setBytes(size, sourceSlice, sourceIndex, length);
        }
        size += length;
    }

    @Override
    public void writeBytes(InputStream in, int length)
            throws IOException
    {
        slice = Slices.ensureSize(slice, size + length);
        slice.setBytes(size, in, length);
        size += length;
    }

    @Override
    public void writeZero(int length)
    {
        slice = Slices.ensureSize(slice, size + length);
        super.writeZero(length);
    }

    @Override
    public CustomSliceOutput appendLong(long value)
    {
        writeLong(value);
        return this;
    }

    @Override
    public CustomSliceOutput appendDouble(double value)
    {
        writeDouble(value);
        return this;
    }

    @Override
    public CustomSliceOutput appendInt(int value)
    {
        writeInt(value);
        return this;
    }

    @Override
    public CustomSliceOutput appendShort(int value)
    {
        writeShort(value);
        return this;
    }

    @Override
    public CustomSliceOutput appendByte(int value)
    {
        writeByte(value);
        return this;
    }

    @Override
    public CustomSliceOutput appendBytes(byte[] source, int sourceIndex, int length)
    {
        write(source, sourceIndex, length);
        return this;
    }

    @Override
    public CustomSliceOutput appendBytes(byte[] source)
    {
        writeBytes(source);
        return this;
    }

    @Override
    public CustomSliceOutput appendBytes(Slice slice)
    {
        writeBytes(slice);
        return this;
    }

    @Override
    public Slice slice()
    {
        return slice.slice(0, size);
    }

    public Slice copySlice()
    {
        Slice copy = Slices.allocate(size);
        slice.getBytes(0, copy);
        return copy;
    }

    @Override
    public Slice getUnderlyingSlice()
    {
        return slice;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("CustomSliceOutput{");
        builder.append("size=").append(size);
        builder.append(", capacity=").append(slice.length());
        builder.append('}');
        return builder.toString();
    }

    @Override
    public String toString(Charset charset)
    {
        return slice.toString(0, size, charset);
    }
}
