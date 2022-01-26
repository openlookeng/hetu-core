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
package io.prestosql.spi.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.DoubleMath;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.Slice;
import io.airlift.slice.UnsafeSlice;
import org.openjdk.jol.info.ClassLayout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.RoundingMode;
import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Double.doubleToLongBits;

/**
 * BloomFilter is a probabilistic data structure for set membership check. BloomFilters are
 * highly space efficient when compared to using a HashSet. Because of the probabilistic nature of
 * bloom filter false positive (element not present in bloom filter but test() says true) are
 * possible but false negatives are not possible (if element is present then test() will never
 * say false). The false positive probability is configurable (default: 5%) depending on which
 * storage requirement may increase or decrease. Lower the false positive probability greater
 * is the space requirement.
 * Bloom filters are sensitive to number of elements that will be inserted in the bloom filter.
 * During the creation of bloom filter expected number of entries must be specified. If the number
 * of insertions exceed the specified initial number of entries then false positive probability will
 * increase accordingly.
 * <p>
 * Internally, this implementation of bloom filter uses Murmur3 fast non-cryptographic hash
 * algorithm. Although Murmur2 is slightly faster than Murmur3 in Java, it suffers from hash
 * collisions for specific sequence of repeating bytes. Check the following link for more info
 * https://code.google.com/p/smhasher/wiki/MurmurHash2Flaw
 * <p>
 * This class was forked from {@code org.apache.orc.util.BloomFilter}.
 */
public class BloomFilter
        implements Serializable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BloomFilter.class).instanceSize() + ClassLayout.parseClass(BitSet.class).instanceSize();

    // from 64-bit linear congruential generator
    private static final long NULL_HASHCODE = 2862933555777941757L;

    private final BitSet bitSet;
    private final int numHashFunctions;
    private int numBits;

    public BloomFilter(long expectedEntries, double fpp)
    {
        checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
        checkArgument(fpp > 0.0 && fpp < 1.0, "False positive probability should be > 0.0 & < 1.0");
        int nb = optimalNumOfBits(expectedEntries, fpp);
        // make 'm' multiple of 64
        this.numBits = nb + (Long.SIZE - (nb % Long.SIZE));
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
        this.bitSet = new BitSet(numBits);
    }

    /**
     * A constructor to support rebuilding the BloomFilter from a serialized representation.
     *
     * @param bits the serialized bits
     * @param numFuncs the number of functions used
     */
    public BloomFilter(long[] bits, int numFuncs)
    {
        bitSet = new BitSet(bits);
        this.numBits = (int) bitSet.bitSize();
        numHashFunctions = numFuncs;
    }

    static int optimalNumOfHashFunctions(long n, long m)
    {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    static int optimalNumOfBits(long n, double p)
    {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    // Thomas Wang's integer hash function
    // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
    private static long getLongHash(long inputKey)
    {
        long key = inputKey;
        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key ^= (key >> 24);
        key = (key + (key << 3)) + (key << 8); // key * 265
        key ^= (key >> 14);
        key = (key + (key << 2)) + (key << 4); // key * 21
        key ^= (key >> 28);
        key += (key << 31);
        return key;
    }

    /**
     * De-serialize a BloomFilter from a given InputStream
     *
     * @param in InputStream that contains serialized value of a BloomFilter
     * @return De-serialized BloomFilter
     * @throws IOException Exception reading values from the InputStream
     */
    public static BloomFilter readFrom(InputStream in)
            throws IOException
    {
        int localNumHashFunctions = 0;
        int localNumBits = 0;
        try {
            DataInputStream dataInputStream = new DataInputStream(in);
            localNumHashFunctions = dataInputStream.readInt();
            localNumBits = dataInputStream.readInt();

            long[] bits = new long[localNumBits];
            for (int i = 0; i < localNumBits; i++) {
                bits[i] = dataInputStream.readLong();
            }
            return new BloomFilter(bits, localNumHashFunctions);
        }
        catch (IOException e) {
            throw new IOException("Failed to deserialize BloomFilter, numHashFunctions: "
                    + localNumHashFunctions
                    + ", numBits: "
                    + localNumBits
                    + ", cause: "
                    + e.getLocalizedMessage());
        }
    }

    /**
     * Merge in another BloomFilter if it's compatible
     *
     * @param that BloomFilter to merge
     */
    public void merge(BloomFilter that)
    {
        if (that == null) {
            throw new IllegalArgumentException("BloomFilter to merge is null");
        }

        if (this == that) {
            return;
        }

        if (numHashFunctions != that.getNumHashFunctions()) {
            throw new IllegalArgumentException("BloomFilter to merge must have same number of hash functions");
        }

        bitSet.merge(that.bitSet);
        this.numBits = (int) bitSet.bitSize();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(getBitSet());
    }

    @Override
    public boolean equals(Object other)
    {
        return (other != null) &&
                (other.getClass() == getClass()) &&
                (numBits == ((BloomFilter) other).numBits) &&
                (numHashFunctions == ((BloomFilter) other).numHashFunctions) &&
                bitSet.equals(((BloomFilter) other).bitSet);
    }

    @Override
    public int hashCode()
    {
        return bitSet.hashCode() + numHashFunctions * 5;
    }

    public void add(byte[] val)
    {
        // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
        // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
        // implement a Bloom filter without any loss in the asymptotic false positive probability'

        // Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
        // in the above paper
        long hash64 = (val == null) ? NULL_HASHCODE : OrcMurmur3.hash64(val);
        addHash(hash64);
    }

    private void addHash(long hash64)
    {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            bitSet.set(pos);
        }
    }

    public void add(long val)
    {
        addHash(getLongHash(val));
    }

    public void add(Slice val)
    {
        byte[] bytesValue = (val == null) ? null : val.getBytes();
        add(bytesValue);
    }

    public void addLong(long val)
    {
        addHash(getLongHash(val));
    }

    public void add(double val)
    {
        addLong(doubleToLongBits(val));
    }

    public void add(float val)
    {
        add((double) val);
    }

    public boolean test(byte[] val)
    {
        long hash64 = (val == null) ? NULL_HASHCODE : OrcMurmur3.hash64(val);
        return testHash(hash64);
    }

    public boolean test(Slice val)
    {
        long hash64 = (val == null) ? NULL_HASHCODE : OrcMurmur3.hash64(val);
        return testHash(hash64);
    }

    private boolean testHash(long hash64)
    {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public boolean test(long val)
    {
        return testHash(getLongHash(val));
    }

    public boolean test(double val)
    {
        return test(doubleToLongBits(val));
    }

    public boolean test(float val)
    {
        return test((double) val);
    }

    public int getNumBits()
    {
        return numBits;
    }

    public int getNumHashFunctions()
    {
        return numHashFunctions;
    }

    public long[] getBitSet()
    {
        return bitSet.getData();
    }

    /**
     * Returns the probability that test() method return true for an object
     * that has not actually been put in the BloomFilter.
     * Ideally, this number should be close to the fpp parameter passed in constructor or smaller.
     * If it is significantly higher, it is usually the case that too many items (more than expected)
     * have been put in the BloomFilter.
     *
     * @return Expected fpp value
     */
    public double expectedFpp()
    {
        return Math.pow((double) bitSet.bitCount() / bitSet.bitSize(), numHashFunctions);
    }

    public long approximateElementCount()
    {
        long bitSize = bitSet.bitSize();
        long bitCount = bitSet.bitCount();

        /**
         * Each insertion is expected to reduce the # of clear bits by a factor of
         * `numHashFunctions/bitSize`. So, after n insertions, expected bitCount is `bitSize * (1 - (1 -
         * numHashFunctions/bitSize)^n)`. Solving that for n, and approximating `ln x` as `x - 1` when x
         * is close to 1 (why?), gives the following formula.
         */
        double fractionOfBitsSet = (double) bitCount / bitSize;
        return DoubleMath.roundToLong(
                -Math.log1p(-fractionOfBitsSet) * bitSize / numHashFunctions, RoundingMode.HALF_UP);
    }

    public boolean isEmpty()
    {
        return bitSet.bitCount() == 0;
    }

    /**
     * Serialize current BloomFilter into an OutputStream
     *
     * @param out OutputStream the BloomFilter is serialized to
     * @throws IOException Exception writing values into the OutputStream
     */
    public void writeTo(OutputStream out)
            throws IOException
    {
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        long[] bits = bitSet.getData();
        dataOutputStream.writeInt(numHashFunctions);
        dataOutputStream.writeInt(bits.length);
        for (int i = 0; i < bits.length; i++) {
            dataOutputStream.writeLong(bits[i]);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("m", numBits)
                .add("k", numHashFunctions)
                .toString();
    }

    /**
     * Bare metal bit set implementation. For performance reasons, this implementation does not check
     * for index bounds nor expand the bit set size if the specified index is greater than the size.
     */
    public static class BitSet
            implements Serializable
    {
        private final long[] data;
        private long bitCount;

        public BitSet(long bits)
        {
            this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
        }

        /**
         * Deserialize long array as bit set.
         *
         * @param data - bit array
         */
        public BitSet(long[] data)
        {
            checkArgument(data.length > 0, "data length is zero");
            this.data = data;
            for (long value : data) {
                bitCount += Long.bitCount(value);
            }
        }

        @Override
        public boolean equals(Object other)
        {
            return (other != null) &&
                    (other.getClass() == getClass()) &&
                    Arrays.equals(this.getData(), ((BitSet) other).getData()) &&
                    (this.bitCount() == ((BitSet) other).bitCount());
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(this.data) + (int) this.bitCount * 5;
        }

        /**
         * Merge bitsets
         *
         * @param that BitSet to merge
         */
        public void merge(BitSet that)
        {
            if (this.data.length != that.data.length) {
                throw new IllegalArgumentException("BitSet to merge must have same length");
            }
            for (int i = 0; i < data.length; i++) {
                long oldBitCount = Long.bitCount(this.data[i]);
                this.data[i] = this.data[i] | that.data[i];
                bitCount += (Long.bitCount(this.data[i]) - oldBitCount);
            }
        }

        /**
         * Sets the bit at specified index.
         *
         * @param index - position
         */
        public void set(int index)
        {
            if (!get(index)) {
                data[index >>> 6] |= (1L << index);
                bitCount++;
            }
        }

        /**
         * Returns true if the bit is set in the specified index.
         *
         * @param index - position
         * @return - value at the bit position
         */
        public boolean get(int index)
        {
            return (data[index >>> 6] & (1L << index)) != 0;
        }

        /**
         * Number of bits
         */
        public long bitSize()
        {
            return (long) data.length * Long.SIZE;
        }

        public long bitCount()
        {
            return bitCount;
        }

        public long[] getData()
        {
            return data;
        }
    }

    /**
     * This class was forked from {@code org.apache.orc.util.Murmur3}.
     */
    @VisibleForTesting
    public static final class OrcMurmur3
    {
        private static final long C1 = 0x87c37b91114253d5L;
        private static final long C2 = 0x4cf5ad432745937fL;
        private static final int R1 = 31;
        private static final int R2 = 27;
        private static final int M = 5;
        private static final int N1 = 0x52dce729;

        private static final int DEFAULT_SEED = 104729;

        private OrcMurmur3() {}

        /**
         * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
         *
         * @param data - input byte array
         * @return - hashcode
         */
        @SuppressWarnings("fallthrough")
        public static long hash64(byte[] data)
        {
            long hash = DEFAULT_SEED;
            int fastLimit = (data.length - SIZE_OF_LONG) + 1;

            // body
            int current = 0;
            while (current < fastLimit) {
                long k = ByteArrays.getLong(data, current);
                current += SIZE_OF_LONG;

                // mix functions
                k *= C1;
                k = Long.rotateLeft(k, R1);
                k *= C2;
                hash ^= k;
                hash = Long.rotateLeft(hash, R2) * M + N1;
            }

            // tail
            long k = 0;
            switch (data.length - current) {
                case 7:
                    k ^= ((long) data[current + 6] & 0xff) << 48;
                    // $FALL-THROUGH$
                case 6:
                    k ^= ((long) data[current + 5] & 0xff) << 40;
                    // $FALL-THROUGH$
                case 5:
                    k ^= ((long) data[current + 4] & 0xff) << 32;
                    // $FALL-THROUGH$
                case 4:
                    k ^= ((long) data[current + 3] & 0xff) << 24;
                    // $FALL-THROUGH$
                case 3:
                    k ^= ((long) data[current + 2] & 0xff) << 16;
                    // $FALL-THROUGH$
                case 2:
                    k ^= ((long) data[current + 1] & 0xff) << 8;
                    // $FALL-THROUGH$
                case 1:
                    k ^= ((long) data[current] & 0xff);
                    k *= C1;
                    k = Long.rotateLeft(k, R1);
                    k *= C2;
                    hash ^= k;
                    // $FALL-THROUGH$
                default:
                    // do nothing
            }

            // finalization
            hash ^= data.length;
            hash = fmix64(hash);

            return hash;
        }

        @SuppressWarnings("fallthrough")
        public static long hash64(Slice data)
        {
            long hash = DEFAULT_SEED;
            int fastLimit = (data.length() - SIZE_OF_LONG) + 1;

            // body
            int current = 0;
            while (current < fastLimit) {
                long k = UnsafeSlice.getLongUnchecked(data, current);
                current += SIZE_OF_LONG;

                // mix functions
                k *= C1;
                k = Long.rotateLeft(k, R1);
                k *= C2;
                hash ^= k;
                hash = Long.rotateLeft(hash, R2) * M + N1;
            }

            // tail
            long k = 0;
            switch (data.length() - current) {
                case 7:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current + 6) & 0xff) << 48;
                    // $FALL-THROUGH$
                case 6:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current + 5) & 0xff) << 40;
                    // $FALL-THROUGH$
                case 5:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current + 4) & 0xff) << 32;
                    // $FALL-THROUGH$
                case 4:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current + 3) & 0xff) << 24;
                    // $FALL-THROUGH$
                case 3:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current + 2) & 0xff) << 16;
                    // $FALL-THROUGH$
                case 2:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current + 1) & 0xff) << 8;
                    // $FALL-THROUGH$
                case 1:
                    k ^= ((long) UnsafeSlice.getByteUnchecked(data, current) & 0xff);
                    k *= C1;
                    k = Long.rotateLeft(k, R1);
                    k *= C2;
                    hash ^= k;
                    // $FALL-THROUGH$
                default:
                    // do noting
            }

            // finalization
            hash ^= data.length();
            hash = fmix64(hash);

            return hash;
        }

        private static long fmix64(long inputValue)
        {
            long h = inputValue;
            h ^= (h >>> 33);
            h *= 0xff51afd7ed558ccdL;
            h ^= (h >>> 33);
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= (h >>> 33);
            return h;
        }
    }
}
