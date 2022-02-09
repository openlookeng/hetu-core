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
package io.prestosql.queryeditorui.output.builders;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.io.CountingOutputStream;
import com.opencsv.CSVWriter;
import io.prestosql.client.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class CsvOutputBuilder
        implements JobOutputBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(CsvOutputBuilder.class);

    private static final String FILE_SUFFIX = ".csv";

    @JsonIgnore
    private final File outputFile;
    @JsonIgnore
    private final CSVWriter csvWriter;
    @JsonIgnore
    private final boolean includeHeader;
    @JsonIgnore
    private final CountingOutputStream countingOutputStream;
    @JsonIgnore
    private final long maxFileSizeBytes;
    @JsonIgnore
    private boolean headerWritten;
    @JsonIgnore
    private final UUID jobUUID;

    public CsvOutputBuilder(boolean includeHeader, UUID jobUUID, long maxFileSizeBytes, boolean compressedOutput) throws IOException
    {
        this.includeHeader = includeHeader;
        this.jobUUID = jobUUID;
        this.outputFile = File.createTempFile(jobUUID.toString(), FILE_SUFFIX);
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.countingOutputStream = new CountingOutputStream(new FileOutputStream(this.outputFile));
        OutputStreamWriter writer;
        if (compressedOutput) {
            writer = new OutputStreamWriter(new GZIPOutputStream(this.countingOutputStream), StandardCharsets.UTF_8);
        }
        else {
            writer = new OutputStreamWriter(this.countingOutputStream, StandardCharsets.UTF_8);
        }
        this.csvWriter = new CSVWriter(writer);
    }

    @Override
    public void addRow(List<Object> row)
            throws FileTooLargeException
    {
        final String[] values = new String[row.size()];
        for (int i = 0; i < values.length; i++) {
            // Display byte array as Hexadecimal in order to keep consistent with OpenLooKeng client
            if (row.get(i) instanceof byte[]) {
                byte[] bytes = (byte[]) row.get(i);
                StringBuilder sb = new StringBuilder();
                for (byte b : bytes) {
                    String hex = Integer.toHexString(b & 0xFF);
                    if (hex.length() < 2) {
                        sb.append(0);
                    }
                    sb.append(hex);
                }
                values[i] = sb.toString();
            }
            else {
                final Object value = row.get(i);
                values[i] = (value == null) ? "" : value.toString();
            }
        }
        writeCsvRow(values);
    }

    @Override
    public void addColumns(List<Column> columns)
            throws FileTooLargeException
    {
        if (!headerWritten && includeHeader) {
            List<String> columnNames = Lists.transform(columns, Column::getName);
            writeCsvRow(columnNames.toArray(new String[columnNames.size()]));
            headerWritten = true;
        }
    }

    @Override
    public String processQuery(String query)
    {
        return query;
    }

    private void writeCsvRow(String[] cols)
            throws FileTooLargeException
    {
        csvWriter.writeNext(cols);

        if (countingOutputStream.getCount() > maxFileSizeBytes) {
            try {
                csvWriter.close();
            }
            catch (IOException e) {
                LOG.error("Caught exception closing csv writer", e);
            }

            delete();
            throw new FileTooLargeException();
        }
    }

    @Override
    public File build()
    {
        try {
            csvWriter.close();
        }
        catch (IOException e) {
            LOG.debug("Error message: " + e.getStackTrace());
        }

        return outputFile;
    }

    @Override
    public void delete()
    {
        LOG.info("Deleting outputFile {}", outputFile);
        if (!outputFile.delete()) {
            LOG.error("Failed to delete outputFile {}", outputFile);
        }
    }
}
