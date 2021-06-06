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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public abstract class MultiChannelGroupBy
{
    private static final int defaultBlockSize = 1024;

    protected final List<Type> hashTypes;
    protected final int[] channels;
    protected final Optional<Integer> inputHashChannel;
    protected final HashGenerator hashGenerator;
    protected final OptionalInt precomputedHashChannel;
    protected final PagesHashStrategy hashStrategy;
    protected final List<ObjectArrayList<Block>> channelBuilders;
    protected final List<Type> types;

    protected DictionaryLookBack dictionaryLookBack;
    protected boolean processDictionary;
    protected long currentPageSizeInBytes;

    public MultiChannelGroupBy(List<? extends Type> hashTypes, int[] hashChannels, Optional<Integer> inputHashChannel, int expectedSize,
                               boolean processDictionary, JoinCompiler joinCompiler)
    {
        this.hashTypes = ImmutableList.copyOf(requireNonNull(hashTypes, "hashTypes is null"));

        requireNonNull(joinCompiler, "joinCompiler is null");
        requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashTypes.size() == hashChannels.length, "hashTypes and hashChannels have different sizes");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : this.hashTypes;
        this.channels = hashChannels.clone();

        this.hashGenerator = inputHashChannel.isPresent() ? new PrecomputedHashGenerator(inputHashChannel.get()) : new InterpretedHashGenerator(this.hashTypes, hashChannels);
        this.processDictionary = processDictionary;

        // For each hashed channel, create an appendable list to hold the blocks (builders).  As we
        // add new values we append them to the existing block builder until it fills up and then
        // we add a new block builder to each list.
        ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channelBuilders = ImmutableList.builder();
        for (int i = 0; i < hashChannels.length; i++) {
            outputChannels.add(i);
            channelBuilders.add(ObjectArrayList.wrap(new Block[defaultBlockSize], 0));
        }
        if (inputHashChannel.isPresent()) {
            this.precomputedHashChannel = OptionalInt.of(hashChannels.length);
            channelBuilders.add(ObjectArrayList.wrap(new Block[defaultBlockSize], 0));
        }
        else {
            this.precomputedHashChannel = OptionalInt.empty();
        }
        this.channelBuilders = channelBuilders.build();
        JoinCompiler.PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(this.types, outputChannels.build());
        hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(this.channelBuilders, this.precomputedHashChannel);
    }

    protected boolean canProcessDictionary(Page page)
    {
        if (!this.processDictionary || channels.length > 1 || !(page.getBlock(channels[0]) instanceof DictionaryBlock)) {
            return false;
        }

        if (inputHashChannel.isPresent()) {
            Block inputHashBlock = page.getBlock(inputHashChannel.get());
            DictionaryBlock inputDataBlock = (DictionaryBlock) page.getBlock(channels[0]);

            if (!(inputHashBlock instanceof DictionaryBlock)) {
                // data channel is dictionary encoded but hash channel is not
                return false;
            }
            // dictionarySourceIds of data block and hash block do not match
            return ((DictionaryBlock) inputHashBlock).getDictionarySourceId().equals(inputDataBlock.getDictionarySourceId());
        }

        return true;
    }

    protected boolean isRunLengthEncoded(Page page)
    {
        for (int i = 0; i < channels.length; i++) {
            if (!(page.getBlock(channels[i]) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }

    protected void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    // For a page that contains DictionaryBlocks, create a new page in which
    // the dictionaries from the DictionaryBlocks are extracted into the corresponding channels
    // From Page(DictionaryBlock1, DictionaryBlock2) create new page with Page(dictionary1, dictionary2)
    private Page createPageWithExtractedDictionary(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        Block dictionary = ((DictionaryBlock) page.getBlock(channels[0])).getDictionary();

        // extract data dictionary
        blocks[channels[0]] = dictionary;

        // extract hash dictionary
        if (inputHashChannel.isPresent()) {
            blocks[inputHashChannel.get()] = ((DictionaryBlock) page.getBlock(inputHashChannel.get())).getDictionary();
        }

        return new Page(dictionary.getPositionCount(), blocks);
    }

    private int getGroupId(HashGenerator hashGenerator, Page page, int positionInDictionary, GroupBy groupBy)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = groupBy.putIfAbsent(positionInDictionary, page, hashGenerator.hashPosition(positionInDictionary, page));
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    protected static final class DictionaryLookBack
            implements Restorable
    {
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, -1);
        }

        public Block getDictionary()
        {
            return dictionary;
        }

        public int getGroupId(int position)
        {
            return processed[position];
        }

        public boolean isProcessed(int position)
        {
            return processed[position] != -1;
        }

        public void setProcessed(int position, int groupId)
        {
            processed[position] = groupId;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            DictionaryLookBack.DictionaryLookBackState myState = new DictionaryLookBack.DictionaryLookBackState();
            SliceOutput sliceOutput = new DynamicSliceOutput(0);
            serdeProvider.getBlockEncodingSerde().writeBlock(sliceOutput, this.dictionary);
            myState.dictionary = sliceOutput.getUnderlyingSlice().getBytes();
            myState.processed = Arrays.copyOf(processed, processed.length);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            DictionaryLookBack.DictionaryLookBackState myState = (DictionaryLookBack.DictionaryLookBackState) state;
            int[] stored = myState.processed;
            for (int i = 0; i < this.processed.length; i++) {
                this.processed[i] = stored[i];
            }
        }

        protected static class DictionaryLookBackState
                implements Serializable
        {
            protected byte[] dictionary;
            private int[] processed;
        }
    }

    protected class AddNonDictionaryPageWork
            implements Work<Void>
    {
        private final Page page;
        private int lastPosition;
        private GroupBy groupBy;

        public AddNonDictionaryPageWork(Page page, GroupBy groupBy)
        {
            this.page = requireNonNull(page, "page is null");
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !groupBy.needMoreCapacity()) {
                // get the group for the current row
                groupBy.putIfAbsent(lastPosition, page);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    protected class AddDictionaryPageWork
            implements Work<Void>
    {
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;
        private int lastPosition;
        private GroupBy groupBy;

        public AddDictionaryPageWork(Page page, GroupBy groupBy)
        {
            verify(canProcessDictionary(page), "invalid call to addDictionaryPage");
            this.page = requireNonNull(page, "page is null");
            this.dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
            updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = createPageWithExtractedDictionary(page);
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !groupBy.needMoreCapacity()) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                getGroupId(hashGenerator, dictionaryPage, positionInDictionary, groupBy);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    protected class AddRunLengthEncodedPageWork
            implements Work<Void>
    {
        private final Page page;

        private boolean finished;
        GroupBy groupBy;

        public AddRunLengthEncodedPageWork(Page page, GroupBy groupBy)
        {
            this.page = requireNonNull(page, "page is null");
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            checkState(!finished);
            if (page.getPositionCount() == 0) {
                finished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupBy.putIfAbsent(0, page);
            finished = true;

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @RestorableConfig(uncapturedFields = {"this$0", "page", "groupBy"})
    protected class GetNonDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>, Restorable
    {
        private final BlockBuilder blockBuilder;
        private final Page page;

        private boolean finished;
        private int lastPosition;
        GroupBy groupBy;

        public GetNonDictionaryGroupIdsWork(Page page, GroupBy groupBy)
        {
            this.page = requireNonNull(page, "page is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !groupBy.needMoreCapacity()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, groupBy.putIfAbsent(lastPosition, page));
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(groupBy.getGroupCount(), blockBuilder.build());
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GetNonDictionaryGroupIdsWorkState myState = new GetNonDictionaryGroupIdsWorkState();
            myState.blockBuilder = blockBuilder.capture(serdeProvider);
            myState.finished = finished;
            myState.lastPosition = lastPosition;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GetNonDictionaryGroupIdsWorkState myState = (GetNonDictionaryGroupIdsWorkState) state;
            this.blockBuilder.restore(myState.blockBuilder, serdeProvider);
            this.finished = myState.finished;
            this.lastPosition = myState.lastPosition;
        }
    }

    private static class GetNonDictionaryGroupIdsWorkState
            implements Serializable
    {
        private Object blockBuilder;
        private boolean finished;
        private int lastPosition;
    }

    protected class GetDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private boolean finished;
        private int lastPosition;
        private GroupBy groupBy;

        public GetDictionaryGroupIdsWork(Page page, GroupBy groupBy)
        {
            this.page = requireNonNull(page, "page is null");
            verify(canProcessDictionary(page), "invalid call to processDictionary");

            this.dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
            updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = createPageWithExtractedDictionary(page);

            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !groupBy.needMoreCapacity()) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                int groupId = getGroupId(hashGenerator, dictionaryPage, positionInDictionary, groupBy);
                BIGINT.writeLong(blockBuilder, groupId);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(groupBy.getGroupCount(), blockBuilder.build());
        }
    }

    protected class GetRunLengthEncodedGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final Page page;

        int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;
        private GroupBy groupBy;

        public GetRunLengthEncodedGroupIdsWork(Page page, GroupBy groupBy)
        {
            this.page = requireNonNull(page, "page is null");
            this.groupBy = groupBy;
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);
            if (page.getPositionCount() == 0) {
                processFinished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupBy.needMoreCapacity() && !groupBy.tryToIncreaseCapacity()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = groupBy.putIfAbsent(0, page);
            processFinished = true;
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            return new GroupByIdBlock(
                    groupBy.getGroupCount(),
                    new RunLengthEncodedBlock(
                            BIGINT.createFixedSizeBlockBuilder(1).writeLong(groupId).build(),
                            page.getPositionCount()));
        }
    }
}
