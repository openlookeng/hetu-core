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
package io.hetu.core.plugin.carbondata.readers;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.util.ByteUtil;

import java.util.Optional;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.prestosql.spi.type.Varchars.byteCount;

/**
 * This class reads the String data and convert it into Slice Block
 */
public class SliceStreamReader
        extends CarbonColumnVectorImpl
        implements HetuCoreVectorBlockBuilder
{
    protected int batchSize;

    protected Type type = VarcharType.VARCHAR;

    protected BlockBuilder builder;

    private Block dictionaryBlock;

    private boolean isLocalDict;

    private int maxLength;

    private boolean isCharType;

    public SliceStreamReader(int batchSize, DataType dataType, int maxLength, boolean isCharType)
    {
        super(batchSize, dataType);
        this.batchSize = batchSize;
        this.builder = type.createBlockBuilder(null, batchSize);
        this.maxLength = maxLength;
        this.isCharType = isCharType;
    }

    @Override
    public Block buildBlock()
    {
        if (dictionaryBlock == null) {
            return builder.build();
        }
        else {
            int[] dataArray;
            if (isLocalDict) {
                dataArray = (int[]) ((CarbonColumnVectorImpl) getDictionaryVector()).getDataArray();
            }
            else {
                dataArray = (int[]) getDataArray();
            }
            return new DictionaryBlock(batchSize, dictionaryBlock, dataArray);
        }
    }

    @Override
    public void setDictionary(CarbonDictionary dictionary)
    {
        super.setDictionary(dictionary);
        if (dictionary == null) {
            dictionaryBlock = null;
            this.isLocalDict = false;
            return;
        }
        boolean[] nulls = new boolean[dictionary.getDictionarySize()];
        nulls[0] = true;
        nulls[1] = true;
        int[] dictOffsets = new int[dictionary.getDictionarySize() + 1];
        int[] truncatedDictOffsets = new int[dictionary.getDictionarySize() + 1];
        int size = 0;
        for (int i = 0; i < dictionary.getDictionarySize(); i++) {
            dictOffsets[i] = size;
            if (dictionary.getDictionaryValue(i) != null) {
                size += dictionary.getDictionaryValue(i).length;
                if (maxLength >= 0) {
                    if (i < 2) {
                        truncatedDictOffsets[i] = dictOffsets[i];
                    }
                    else {
                        int actualLength = dictionary.getDictionaryValue(i).length;
                        int truncatedLength = computeTruncatedLength(wrappedBuffer(dictionary.getDictionaryValue(i)), 0, actualLength, maxLength, isCharType);
                        truncatedDictOffsets[i + 1] = truncatedDictOffsets[i] + truncatedLength;
                    }
                }
            }
        }
        byte[] singleArrayDictValues = new byte[size];
        byte[] truncatedSingleArrayDictValues = new byte[size];
        for (int i = 0; i < dictionary.getDictionarySize(); i++) {
            if (dictionary.getDictionaryValue(i) != null) {
                System.arraycopy(dictionary.getDictionaryValue(i), 0, singleArrayDictValues, dictOffsets[i],
                        dictionary.getDictionaryValue(i).length);
                if (maxLength >= 0) {
                    System.arraycopy(dictionary.getDictionaryValue(i), 0, truncatedSingleArrayDictValues, truncatedDictOffsets[i],
                            dictionary.getDictionaryValue(i).length);
                }
            }
        }
        if (maxLength < 0) {
            dictOffsets[dictOffsets.length - 1] = size;
            dictionaryBlock = new VariableWidthBlock(dictionary.getDictionarySize(),
                    Slices.wrappedBuffer(singleArrayDictValues), dictOffsets, Optional.of(nulls));
        }
        else {
            dictionaryBlock = new VariableWidthBlock(dictionary.getDictionarySize(),
                    Slices.wrappedBuffer(truncatedSingleArrayDictValues), truncatedDictOffsets, Optional.of(nulls));
        }
        this.isLocalDict = true;
    }

    @Override
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    @Override
    public void putByteArray(int rowId, byte[] value)
    {
        type.writeSlice(builder, wrappedBuffer(value));
    }

    @Override
    public void putByteArray(int rowId, int offset, int length, byte[] value)
    {
        int truncatedLength = computeTruncatedLength(wrappedBuffer(value), offset, length, maxLength, isCharType);
        type.writeSlice(builder, wrappedBuffer(value), offset, truncatedLength);
    }

    @Override
    public void putByteArray(int rowId, int count, byte[] value)
    {
        for (int i = 0; i < count; i++) {
            type.writeSlice(builder, wrappedBuffer(value));
        }
    }

    @Override
    public void putNull(int rowId)
    {
        if (dictionaryBlock == null) {
            builder.appendNull();
        }
    }

    @Override
    public void putNulls(int rowId, int count)
    {
        if (dictionaryBlock == null) {
            for (int i = 0; i < count; ++i) {
                builder.appendNull();
            }
        }
    }

    @Override
    public void reset()
    {
        builder = type.createBlockBuilder(null, batchSize);
    }

    @Override
    public void putObject(int rowId, Object value)
    {
        if (value == null) {
            putNull(rowId);
        }
        else {
            if (dictionaryBlock == null) {
                putByteArray(rowId, ByteUtil.toBytes((String) value));
            }
            else {
                putInt(rowId, (int) value);
            }
        }
    }

    private int computeTruncatedLength(Slice slice, int offset, int length, int maxCodePointCount, boolean isCharType)
    {
        if (isCharType) {
            // truncate the characters and then remove the trailing white spaces
            return byteCountWithoutTrailingSpace(slice, offset, length, maxCodePointCount);
        }
        if (maxCodePointCount >= 0 && length > maxCodePointCount) {
            return byteCount(slice, offset, length, maxCodePointCount);
        }
        return length;
    }
}
