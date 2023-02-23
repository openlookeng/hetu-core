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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.geospatial.Rectangle;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinCompiler.LookupSourceSupplierFactory;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.prestosql.sql.gen.OrderingCompiler;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.inject.Inject;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static io.prestosql.operator.SyntheticAddress.encodeSyntheticAddress;
import static java.util.Objects.requireNonNull;

/**
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #createLookupSourceSupplier} method</li>
 * <li>Positional output via the {@link #appendTo} method</li>
 * </ul>
 */
@RestorableConfig(uncapturedFields = {"orderingCompiler", "joinCompiler", "metadata", "types"})
public class PagesIndex
        implements Swapper, Restorable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesIndex.class).instanceSize();
    private static final Logger log = Logger.get(PagesIndex.class);

    private final OrderingCompiler orderingCompiler;
    private final JoinCompiler joinCompiler;
    private final Metadata metadata;

    private final List<Type> types;
    private final LongArrayList valueAddresses;
    private final ObjectArrayList<Block>[] channels;
    private final boolean eagerCompact;

    private int nextBlockToCompact;
    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;

    private PagesIndex(
            OrderingCompiler orderingCompiler,
            JoinCompiler joinCompiler,
            Metadata metadata,
            List<Type> types,
            int expectedPositions,
            boolean eagerCompact)
    {
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.valueAddresses = new LongArrayList(expectedPositions);
        this.eagerCompact = eagerCompact;

        //noinspection rawtypes
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }

        estimatedSize = calculateEstimatedSize();
    }

    public interface Factory
    {
        PagesIndex newPagesIndex(List<Type> types, int expectedPositions);
    }

    public static class TestingFactory
            implements Factory
    {
        private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler();
        private static final Metadata METADATA = createTestMetadataManager();
        private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(METADATA);
        private final boolean eagerCompact;

        public TestingFactory(boolean eagerCompact)
        {
            this.eagerCompact = eagerCompact;
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(ORDERING_COMPILER, JOIN_COMPILER, METADATA, types, expectedPositions, eagerCompact);
        }
    }

    public static class DefaultFactory
            implements Factory
    {
        private final OrderingCompiler orderingCompiler;
        private final JoinCompiler joinCompiler;
        private final boolean eagerCompact;
        private final Metadata metadata;

        @Inject
        public DefaultFactory(OrderingCompiler orderingCompiler, JoinCompiler joinCompiler, FeaturesConfig featuresConfig, Metadata metadata)
        {
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.eagerCompact = requireNonNull(featuresConfig, "featuresConfig is null").isPagesIndexEagerCompactionEnabled();
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(orderingCompiler, joinCompiler, metadata, types, expectedPositions, eagerCompact);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public LongArrayList getValueAddresses()
    {
        return valueAddresses;
    }

    public ObjectArrayList<Block> getChannel(int channel)
    {
        return channels[channel];
    }

    public void clear()
    {
        for (ObjectArrayList<Block> channel : channels) {
            channel.clear();
            channel.trim();
        }
        valueAddresses.clear();
        valueAddresses.trim();
        positionCount = 0;
        nextBlockToCompact = 0;
        pagesMemorySize = 0;

        estimatedSize = calculateEstimatedSize();
    }

    public void addPage(Page page)
    {
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        positionCount += page.getPositionCount();

        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);
            valueAddresses.add(sliceAddress);
        }
        estimatedSize = calculateEstimatedSize();
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, BYTE);
    }

    public void compact()
    {
        if (eagerCompact) {
            return;
        }
        for (int channel = 0; channel < types.size(); channel++) {
            ObjectArrayList<Block> blocks = channels[channel];
            for (int i = nextBlockToCompact; i < blocks.size(); i++) {
                Block block = blocks.get(i);

                // Copy the block to compact its size
                Block compactedBlock = block.copyRegion(0, block.getPositionCount());
                blocks.set(i, compactedBlock);
                pagesMemorySize -= block.getRetainedSizeInBytes();
                pagesMemorySize += compactedBlock.getRetainedSizeInBytes();
            }
        }
        nextBlockToCompact = channels[0].size();
        estimatedSize = calculateEstimatedSize();
    }

    private long calculateEstimatedSize()
    {
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        long channelsArraySize = elementsSize * channels.length;
        long addressesArraySize = sizeOf(valueAddresses.elements());
        return INSTANCE_SIZE + pagesMemorySize + channelsArraySize + addressesArraySize;
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    @Override
    public void swap(int a, int b)
    {
        long[] elements = valueAddresses.elements();
        long temp = elements[a];
        elements[a] = elements[b];
        elements[b] = temp;
    }

    public int buildPage(int position, int[] outputChannels, PageBuilder pageBuilder)
    {
        int positionValue = position;
        while (!pageBuilder.isFull() && positionValue < positionCount) {
            long pageAddress = valueAddresses.getLong(positionValue);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            // append the row
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.length; i++) {
                int outputChannel = outputChannels[i];
                Type type = types.get(outputChannel);
                Block block = this.channels[outputChannel].get(blockIndex);
                type.appendTo(block, blockPosition, pageBuilder.getBlockBuilder(i));
            }

            positionValue++;
        }

        return positionValue;
    }

    public void appendTo(int channel, int position, BlockBuilder output)
    {
        long pageAddress = valueAddresses.getLong(position);

        Type type = types.get(channel);
        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        type.appendTo(block, blockPosition, output);
    }

    public boolean isNull(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.isNull(blockPosition);
    }

    public boolean getBoolean(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getBoolean(block, blockPosition);
    }

    public long getLong(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getLong(block, blockPosition);
    }

    public double getDouble(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getDouble(block, blockPosition);
    }

    public Slice getSlice(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getSlice(block, blockPosition);
    }

    public Object getObject(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getObject(block, blockPosition);
    }

    public Block getSingleValueBlock(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.getSingleValueBlock(blockPosition);
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        sort(sortChannels, sortOrders, 0, getPositionCount());
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders, int startPosition, int endPosition)
    {
        createPagesIndexComparator(sortChannels, sortOrders).sort(this, startPosition, endPosition);
    }

    public boolean positionEqualsPosition(PagesHashStrategy partitionHashStrategy, int leftPosition, int rightPosition)
    {
        long leftAddress = valueAddresses.getLong(leftPosition);
        int leftPageIndex = decodeSliceIndex(leftAddress);
        int leftPagePosition = decodePosition(leftAddress);

        long rightAddress = valueAddresses.getLong(rightPosition);
        int rightPageIndex = decodeSliceIndex(rightAddress);
        int rightPagePosition = decodePosition(rightAddress);

        return partitionHashStrategy.positionEqualsPosition(leftPageIndex, leftPagePosition, rightPageIndex, rightPagePosition);
    }

    public boolean positionEqualsRow(PagesHashStrategy pagesHashStrategy, int indexPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = valueAddresses.getLong(indexPosition);
        int pageIndex = decodeSliceIndex(pageAddress);
        int pagePosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRow(pageIndex, pagePosition, rightPosition, rightPage);
    }

    private PagesIndexOrdering createPagesIndexComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        return orderingCompiler.compilePagesIndexOrdering(sortTypes, sortChannels, sortOrders);
    }

    public Supplier<LookupSource> createLookupSourceSupplier(Session session, List<Integer> joinChannels)
    {
        return createLookupSourceSupplier(session, joinChannels, OptionalInt.empty(), Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, OptionalInt hashChannel)
    {
        return createPagesHashStrategy(joinChannels, hashChannel, Optional.empty());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, OptionalInt hashChannel, Optional<List<Integer>> outputChannels)
    {
        try {
            return joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, outputChannels)
                    .createPagesHashStrategy(ImmutableList.copyOf(channels), hashChannel);
        }
        catch (Exception e) {
            log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
        }

        // if compilation fails, use interpreter
        return new SimplePagesHashStrategy(
                types,
                outputChannels.orElse(rangeList(types.size())),
                ImmutableList.copyOf(channels),
                joinChannels,
                hashChannel,
                Optional.empty(),
                Optional.empty(),
                metadata);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories)
    {
        return createLookupSourceSupplier(session, joinChannels, hashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.empty());
    }

    public PagesSpatialIndexSupplier createPagesSpatialIndex(
            Session session,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            List<Integer> outputChannels,
            Map<Integer, Rectangle> partitions)
    {
        // TODO probably shouldn't copy to reduce memory and for memory accounting's sake
        List<List<Block>> channelLists = ImmutableList.copyOf(this.channels);
        return new PagesSpatialIndexSupplier(session, valueAddresses, types, outputChannels, channelLists, geometryChannel, radiusChannel, partitionChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels)
    {
        return createLookupSourceSupplier(session, joinChannels, hashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, outputChannels, Optional.empty(), Optional.empty());
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels,
            Optional<Integer> countChannel,
            Optional<AggregationBuilder> aggregationBuilder)
    {
        List<List<Block>> channelLists = ImmutableList.copyOf(this.channels);
        if (!joinChannels.isEmpty()) {
            // todo compiled implementation of lookup join does not support when we are joining with empty join channelLists.
            // This code path will trigger only for OUTER joins. To fix that we need to add support for
            //        OUTER joins into NestedLoopsJoin and remove "type == INNER" condition in LocalExecutionPlanner.visitJoin()

            try {
                LookupSourceSupplierFactory lookupSourceFactory = joinCompiler.compileLookupSourceFactory(types, joinChannels, sortChannel, outputChannels, countChannel, aggregationBuilder);
                return lookupSourceFactory.createLookupSourceSupplier(
                        session,
                        valueAddresses,
                        channelLists,
                        hashChannel,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories,
                        aggregationBuilder);
            }
            catch (Exception e) {
                log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
            }
        }

        // if compilation fails
        PagesHashStrategy hashStrategy = new SimplePagesHashStrategy(
                types,
                outputChannels.orElse(rangeList(types.size())),
                channelLists,
                joinChannels,
                hashChannel,
                sortChannel,
                aggregationBuilder,
                metadata);

        return new JoinHashSupplier(
                session,
                hashStrategy,
                valueAddresses,
                channelLists,
                filterFunctionFactory,
                sortChannel,
                searchFunctionFactories,
                OptionalInt.empty());
    }

    private List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("types", types)
                .add("estimatedSize", estimatedSize)
                .toString();
    }

    public Iterator<Page> getPages()
    {
        return new AbstractIterator<Page>()
        {
            private int pageCounter;

            @Override
            protected Page computeNext()
            {
                if (pageCounter == channels[0].size()) {
                    return endOfData();
                }

                Block[] blocks = Stream.of(channels)
                        .map(channel -> channel.get(pageCounter))
                        .toArray(Block[]::new);
                pageCounter++;
                return new Page(blocks);
            }
        };
    }

    public Iterator<Page> getSortedPages()
    {
        return new AbstractIterator<Page>()
        {
            private int currentPosition;
            private final PageBuilder pageBuilder = new PageBuilder(types);
            private final int[] outputChannels = new int[types.size()];

            {
                Arrays.setAll(outputChannels, IntUnaryOperator.identity());
            }

            @Override
            public Page computeNext()
            {
                currentPosition = buildPage(currentPosition, outputChannels, pageBuilder);
                if (pageBuilder.isEmpty()) {
                    return endOfData();
                }
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
        };
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        BlockEncodingSerde blockSerde = serdeProvider.getBlockEncodingSerde();
        PagesIndexState myState = new PagesIndexState();
        myState.valueAddresses = new long[valueAddresses.size()];
        valueAddresses.getElements(0, myState.valueAddresses, 0, valueAddresses.size());
        myState.channels = new byte[channels.length][][];
        for (int i = 0; i < channels.length; i++) {
            int arraySize = channels[i].size();
            myState.channels[i] = new byte[arraySize][];
            Block[] blockArray = new Block[arraySize];
            channels[i].getElements(0, blockArray, 0, arraySize);
            for (int j = 0; j < arraySize; j++) {
                SliceOutput sliceOutput = new DynamicSliceOutput(0);
                blockSerde.writeBlock(sliceOutput, blockArray[j]);
                myState.channels[i][j] = sliceOutput.getUnderlyingSlice().getBytes();
            }
        }
        myState.nextBlockToCompact = nextBlockToCompact;
        myState.positionCount = positionCount;
        myState.pagesMemorySize = pagesMemorySize;
        myState.estimatedSize = estimatedSize;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BlockEncodingSerde blockSerde = serdeProvider.getBlockEncodingSerde();
        PagesIndexState myState = (PagesIndexState) state;
        this.valueAddresses.clear();
        this.valueAddresses.trim();
        this.valueAddresses.addAll(0, new LongArrayList(myState.valueAddresses));
        for (int i = 0; i < myState.channels.length; i++) {
            this.channels[i].clear();
            this.channels[i].trim();
            for (byte[] blockState : myState.channels[i]) {
                Slice input = Slices.wrappedBuffer(blockState);
                this.channels[i].add(blockSerde.readBlock(input.getInput()));
            }
        }
        this.nextBlockToCompact = myState.nextBlockToCompact;
        this.positionCount = myState.positionCount;
        this.pagesMemorySize = myState.pagesMemorySize;
        this.estimatedSize = myState.estimatedSize;
    }

    private static class PagesIndexState
            implements Serializable
    {
        private long[] valueAddresses;
        private byte[][][] channels;
        private int nextBlockToCompact;
        private int positionCount;
        private long pagesMemorySize;
        private long estimatedSize;
    }
}
