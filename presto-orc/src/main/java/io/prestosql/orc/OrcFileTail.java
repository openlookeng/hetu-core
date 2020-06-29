/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.orc;

import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.ExceptionWrappingMetadataReader;
import io.prestosql.orc.metadata.Footer;
import io.prestosql.orc.metadata.Metadata;
import io.prestosql.orc.metadata.MetadataReader;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcMetadataReader;
import io.prestosql.orc.metadata.PostScript;
import io.prestosql.orc.stream.OrcChunkLoader;
import io.prestosql.orc.stream.OrcInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class OrcFileTail
{
    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;
    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;

    private static final Logger log = Logger.get(OrcFileTail.class);

    private Optional<OrcDecompressor> decompressor;
    private PostScript postScript;
    private Metadata metadata;
    private Footer footer;

    private OrcFileTail()
    {
        //nothing to initialize here.
    }

    public Optional<OrcDecompressor> getDecompressor()
    {
        return decompressor;
    }

    public PostScript getPostScript()
    {
        return postScript;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public Footer getFooter()
    {
        return footer;
    }

    public static OrcFileTail readFrom(OrcDataSource orcDataSource,
                                       Optional<OrcWriteValidation> writeValidation) throws IOException
    {
        OrcFileTail orcFileTail = new OrcFileTail();
        //
        // Read the file tail:
        //
        // variable: Footer
        // variable: Metadata
        // variable: PostScript - contains length of footer and metadata
        // 1 byte: postScriptSize

        // figure out the size of the file using the option or filesystem
        long size = orcDataSource.getSize();
        if (size <= PostScript.MAGIC.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid file size %s", size);
        }

        // Read the tail of the file
        int expectedBufferSize = toIntExact(min(size, EXPECTED_FOOTER_SIZE));
        Slice buffer = orcDataSource.readFully(size - expectedBufferSize, expectedBufferSize);

        // get length of PostScript - last byte of the file
        int postScriptSize = buffer.getUnsignedByte(buffer.length() - SIZE_OF_BYTE);
        if (postScriptSize >= buffer.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid postscript length %s", postScriptSize);
        }
        MetadataReader metadataReader = new ExceptionWrappingMetadataReader(orcDataSource.getId(),
                new OrcMetadataReader());

        // decode the post script
        try {
            orcFileTail.postScript = metadataReader.readPostScript(
                    buffer.slice(buffer.length() - SIZE_OF_BYTE - postScriptSize, postScriptSize).getInput());
        }
        catch (OrcCorruptionException e) {
            // check if this is an ORC file and not an RCFile or something else
            if (!isValidHeaderMagic(orcDataSource)) {
                throw new OrcCorruptionException(orcDataSource.getId(), "Not an ORC file");
            }
            throw e;
        }

        // verify this is a supported version
        checkOrcVersion(orcDataSource, orcFileTail.postScript.getVersion());
        validateWrite(validation -> validation.getVersion().equals(orcFileTail.postScript.getVersion()),
                writeValidation, orcDataSource, "Unexpected version");

        int bufferSize = toIntExact(orcFileTail.postScript.getCompressionBlockSize());

        // check compression codec is supported
        CompressionKind compressionKind = orcFileTail.postScript.getCompression();
        orcFileTail.decompressor = OrcDecompressor.createOrcDecompressor(orcDataSource.getId(), compressionKind,
                bufferSize);
        validateWrite(validation -> validation.getCompression() == compressionKind, writeValidation, orcDataSource,
                "Unexpected compression");

        PostScript.HiveWriterVersion hiveWriterVersion = orcFileTail.postScript.getHiveWriterVersion();

        int footerSize = toIntExact(orcFileTail.postScript.getFooterLength());
        int metadataSize = toIntExact(orcFileTail.postScript.getMetadataLength());

        // check if extra bytes need to be read
        Slice completeFooterSlice;
        int completeFooterSize = footerSize + metadataSize + postScriptSize + SIZE_OF_BYTE;
        if (completeFooterSize > buffer.length()) {
            // initial read was not large enough, so just read again with the correct size
            completeFooterSlice = orcDataSource.readFully(size - completeFooterSize, completeFooterSize);
        }
        else {
            // footer is already in the bytes in buffer, just adjust position, length
            completeFooterSlice = buffer.slice(buffer.length() - completeFooterSize, completeFooterSize);
        }

        // read metadata
        Slice metadataSlice = completeFooterSlice.slice(0, metadataSize);
        try (InputStream metadataInputStream = new OrcInputStream(
                OrcChunkLoader.create(orcDataSource.getId(), metadataSlice, orcFileTail.decompressor,
                        newSimpleAggregatedMemoryContext()))) {
            orcFileTail.metadata = metadataReader.readMetadata(hiveWriterVersion, metadataInputStream);
        }

        // read footer
        Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);
        try (InputStream footerInputStream = new OrcInputStream(
                OrcChunkLoader.create(orcDataSource.getId(), footerSlice, orcFileTail.decompressor,
                        newSimpleAggregatedMemoryContext()))) {
            orcFileTail.footer = metadataReader.readFooter(hiveWriterVersion, footerInputStream);
        }
        if (orcFileTail.footer.getTypes().size() == 0) {
            throw new OrcCorruptionException(orcDataSource.getId(), "File has no columns");
        }

        validateWrite(validation -> validation.getColumnNames().equals(
                orcFileTail.footer.getTypes().get(new OrcColumnId(0)).getFieldNames()), writeValidation, orcDataSource,
                "Unexpected column names");
        validateWrite(validation -> validation.getRowGroupMaxRowCount() == orcFileTail.footer.getRowsInRowGroup(),
                writeValidation, orcDataSource, "Unexpected rows in group");
        if (writeValidation.isPresent()) {
            writeValidation.get().validateMetadata(orcDataSource.getId(), orcFileTail.footer.getUserMetadata());
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), orcFileTail.footer.getFileStats());
            writeValidation.get().validateStripeStatistics(orcDataSource.getId(), orcFileTail.footer.getStripes(),
                    orcFileTail.metadata.getStripeStatsList());
        }
        return orcFileTail;
    }

    /**
     * Does the file start with the ORC magic bytes?
     */
    private static boolean isValidHeaderMagic(OrcDataSource source)
            throws IOException
    {
        Slice headerMagic = source.readFully(0, PostScript.MAGIC.length());
        return PostScript.MAGIC.equals(headerMagic);
    }

    /**
     * check the orc version, if it's newer than current version then warn the user.
     */
    private static void checkOrcVersion(OrcDataSource orcDataSource, List<Integer> version)
    {
        if (version.size() > 0) {
            int majorVersion = version.get(0);
            int minorVersion = version.size() > 1 ? version.get(1) : 0;
            if (majorVersion > CURRENT_MAJOR_VERSION || (majorVersion == CURRENT_MAJOR_VERSION && minorVersion > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file %s is written by a newer Hive version %s. Current Hive version is %s.%s.",
                        orcDataSource, Joiner.on('.').join(version), CURRENT_MAJOR_VERSION, CURRENT_MINOR_VERSION);
            }
        }
    }

    private static void validateWrite(Predicate<OrcWriteValidation> test, Optional<OrcWriteValidation> writeValidation,
                                      OrcDataSource orcDataSource, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }
}
