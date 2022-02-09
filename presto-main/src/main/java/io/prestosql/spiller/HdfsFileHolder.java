package io.prestosql.spiller;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;

import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
final class HdfsFileHolder implements Closeable {

    private final Path filePath;
    private final FileSystem hdfsFS;

    @GuardedBy("this")
    private boolean deleted;

    HdfsFileHolder(Path filePath, FileSystem hdfsFS) {
        this.filePath = filePath;
        this.hdfsFS = hdfsFS;
    }

    public synchronized FSDataOutputStream newOutputStream()
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        return this.hdfsFS.create(filePath);
    }

    public synchronized InputStream newInputStream()
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        return this.hdfsFS.open(filePath);
    }

    @Override
    public void close() throws IOException {
        if (deleted) {
            return;
        }
        deleted = true;
        try {
            this.hdfsFS.delete(filePath, true);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path getFilePath()
    {
        return filePath;
    }
}
