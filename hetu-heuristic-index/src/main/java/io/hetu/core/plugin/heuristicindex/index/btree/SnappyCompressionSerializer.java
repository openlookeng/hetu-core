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
package io.hetu.core.plugin.heuristicindex.index.btree;
import org.mapdb.DBException;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializer;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Comparator;

/**
 * This is class is heavily influenced by SerializerCompressionWrapper. The serialize and deserialize api's
 * have been changed to use it Snappy.
 * @param <E>
 */
public class SnappyCompressionSerializer<E>
        implements GroupSerializer<E>
{
    protected final GroupSerializer<E> serializer;

    public SnappyCompressionSerializer(GroupSerializer<E> serializer)
    {
        this.serializer = serializer;
    }

    public int valueArraySearch(Object keys, E key)
    {
        return this.serializer.valueArraySearch(keys, key);
    }

    public int valueArraySearch(Object keys, E key, Comparator comparator)
    {
        return this.serializer.valueArraySearch(keys, key, comparator);
    }

    @Override
    public void serialize(DataOutput2 out, E value)
            throws IOException
    {
        //serialize object
        try (DataOutput2 serializedOutput = new DataOutput2()) {
            serializer.serialize(serializedOutput, value);

            //compress
            try (DataOutput2 compressedOutput = new DataOutput2()) {
                compressedOutput.ensureAvail(serializedOutput.pos + 40);
                int newLen = 0;
                try {
                    newLen = Snappy.compress(serializedOutput.buf, 0, serializedOutput.pos, compressedOutput.buf, 0);
                }
                catch (Exception e) {
                    newLen = 0;
                }
                if (newLen < serializedOutput.pos && newLen != 0) {
                    out.packInt(newLen + 1);
                    out.write(compressedOutput.buf, 0, newLen);
                }
                else {
                    out.packInt(0);
                    out.write(serializedOutput.buf, 0, serializedOutput.pos);
                }
            }
        }
    }

    @Override
    public E deserialize(DataInput2 in, int available)
            throws IOException
    {
        int unpackedSize = in.unpackInt() - 1;
        if (unpackedSize == -1) {
            return this.serializer.deserialize(in, available > 0 ? available - 1 : available);
        }
        else {
            byte[] unpacked = new byte[unpackedSize];
            byte[] input = in.internalByteArray();
            Snappy.uncompress(input, 0, input.length, unpacked, 0);
            DataInput2.ByteArray in2 = new DataInput2.ByteArray(unpacked);
            E ret = this.serializer.deserialize(in2, unpackedSize);
            if (in2.pos != unpackedSize) {
                throw new DBException.DataCorruption("data were not fully read");
            }
            else {
                return ret;
            }
        }
    }

    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException
    {
        try (DataOutput2 out2 = new DataOutput2()) {
            this.serializer.valueArraySerialize(out2, vals);
            if (out2.pos != 0) {
                byte[] tmp = new byte[out2.pos + 40];

                int newLen;
                try {
                    newLen = Snappy.compress(out2.buf, 0, out2.pos, tmp, 0);
                }
                catch (IndexOutOfBoundsException var7) {
                    newLen = 0;
                }
                if (newLen < out2.pos && newLen != 0) {
                    out.packInt(newLen + 1);
                    out.write(tmp, 0, newLen);
                }
                else {
                    out.packInt(0);
                    out.write(out2.buf, 0, out2.pos);
                }
            }
        }
    }

    public Object valueArrayDeserialize(DataInput2 in, int size) throws IOException
    {
        if (size == 0) {
            return this.serializer.valueArrayEmpty();
        }
        else {
            int length = in.unpackInt() - 1;
            if (length == -1) {
                return this.serializer.valueArrayDeserialize(in, size);
            }
            else {
                byte[] copyOf = new byte[length];
                in.readFully(copyOf, 0, length);
                byte[] unpacked = Snappy.uncompress(copyOf);

                DataInput2.ByteArray in2 = new DataInput2.ByteArray(unpacked);
                Object ret = this.serializer.valueArrayDeserialize(in2, size);
                if (in2.pos != unpacked.length) {
                    throw new DBException.DataCorruption("data were not fully read");
                }
                else {
                    return ret;
                }
            }
        }
    }

    public E valueArrayGet(Object vals, int pos)
    {
        return this.serializer.valueArrayGet(vals, pos);
    }

    public int valueArraySize(Object vals)
    {
        return this.serializer.valueArraySize(vals);
    }

    public Object valueArrayEmpty()
    {
        return this.serializer.valueArrayEmpty();
    }

    public Object valueArrayPut(Object vals, int pos, E newValue)
    {
        return this.serializer.valueArrayPut(vals, pos, newValue);
    }

    public Object valueArrayUpdateVal(Object vals, int pos, E newValue)
    {
        return this.serializer.valueArrayUpdateVal(vals, pos, newValue);
    }

    public Object valueArrayFromArray(Object[] objects)
    {
        return this.serializer.valueArrayFromArray(objects);
    }

    public Object valueArrayCopyOfRange(Object vals, int from, int to)
    {
        return this.serializer.valueArrayCopyOfRange(vals, from, to);
    }

    public Object valueArrayDeleteValue(Object vals, int pos)
    {
        return this.serializer.valueArrayDeleteValue(vals, pos);
    }

    public boolean equals(E a1, E a2)
    {
        return this.serializer.equals(a1, a2);
    }

    public int hashCode(E e, int seed)
    {
        return this.serializer.hashCode(e, seed);
    }

    public int compare(E o1, E o2)
    {
        return this.serializer.compare(o1, o2);
    }
}
