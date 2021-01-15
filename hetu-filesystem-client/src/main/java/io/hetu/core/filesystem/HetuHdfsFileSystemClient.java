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
package io.hetu.core.filesystem;

import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.prestosql.spi.filesystem.SupportedFileAttributes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

/**
 * HetuFileSystemClient implementation for HDFS
 *
 * @since 2020-03-30
 */
public class HetuHdfsFileSystemClient
        extends AbstractWorkspaceFileSystemClient
{
    private static final Logger LOG = Logger.get(HetuHdfsFileSystemClient.class);
    private static final String ERROR_MSG_INITIATED_FAILED = "GSS initiate failed";
    private static final String ERROR_MSG_NON_EMPTY_FOR_LOCAL_USE = "Directory .* is not empty";

    private FileSystem hdfs;
    private Configuration hdfsConfig;

    /**
     * Instantiate an HetuHdfsFileSystemClient instance with the passed in parameters
     *
     * @param config {@link HdfsConfig} object to configure the client
     * @param allowAccessRoot The workspace allowAccessRoot directory
     */
    public HetuHdfsFileSystemClient(HdfsConfig config, Path allowAccessRoot)
            throws IOException
    {
        super(allowAccessRoot);
        this.hdfsConfig = config.getHadoopConfig();
        this.hdfs = FileSystem.get(hdfsConfig);
    }

    /**
     * Utility to convert java.nio.Path to org.apache.hadoop.fs.Path.
     *
     * @param path java.nio.Path object
     * @return org.apache.hadoop.fs.Path object
     */
    public static org.apache.hadoop.fs.Path toHdfsPath(Path path)
    {
        return new org.apache.hadoop.fs.Path(path.toString());
    }

    @Override
    public Path createDirectories(Path dir)
            throws IOException
    {
        validate(dir);
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(dir);
        unwrapHdfsExceptions(() -> getHdfs().mkdirs(hdfsPath));
        return dir;
    }

    @Override
    public Path createDirectory(Path dir)
            throws IOException
    {
        validate(dir);
        org.apache.hadoop.fs.Path hdfsParent = toHdfsPath(dir).getParent();
        checkFileExists(hdfsParent);

        // Throws FileAlreadyExistsException if the directory is already there
        if (exists(dir)) {
            throw new FileAlreadyExistsException(dir.toString());
        }
        return createDirectories(dir);
    }

    @Override
    public void delete(Path path)
            throws IOException
    {
        validate(path);
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        checkFileExists(hdfsPath);
        unwrapHdfsExceptions(() -> getHdfs().delete(hdfsPath, false));
    }

    @Override
    public boolean deleteIfExists(Path path)
            throws IOException
    {
        validate(path);
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        return unwrapHdfsExceptions(() -> getHdfs().delete(hdfsPath, false));
    }

    @Override
    public boolean deleteRecursively(Path path)
            throws IOException
    {
        validate(path);
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        return unwrapHdfsExceptions(() -> getHdfs().delete(hdfsPath, true));
    }

    @Override
    public boolean exists(Path path)
    {
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        try {
            return getHdfs().exists(hdfsPath);
        }
        catch (IOException e) {
            return false;
        }
    }

    @Override
    public void move(Path source, Path target)
            throws IOException
    {
        validate(source);
        validate(target);
        org.apache.hadoop.fs.Path hdfsSource = toHdfsPath(source);
        org.apache.hadoop.fs.Path hdfsTarget = toHdfsPath(target);
        checkFileExists(hdfsSource);
        unwrapHdfsExceptions(() -> getHdfs().rename(hdfsSource, hdfsTarget));
    }

    @Override
    public InputStream newInputStream(Path path)
            throws IOException
    {
        validate(path);
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        return unwrapHdfsExceptions(() -> getHdfs().open(hdfsPath));
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options)
            throws IOException
    {
        validate(path);
        // Throw exception if parent directory does not exist: keep same behavior as Files.newOutPutStream
        checkFileExists(toHdfsPath(path.getParent()));
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        if (options.length == 0) {
            return unwrapHdfsExceptions(() -> getHdfs().create(hdfsPath, true));
        }
        else if (options.length == 1 && options[0] == CREATE_NEW) {
            return unwrapHdfsExceptions(() -> getHdfs().create(hdfsPath, false));
        }
        else {
            throw new UnsupportedOperationException("Provided OpenOptions are not supported by HDFS.");
        }
    }

    @Override
    public Object getAttribute(Path path, String attribute)
            throws IOException
    {
        validate(path);
        if (!SupportedFileAttributes.SUPPORTED_ATTRIBUTES.contains(attribute)) {
            throw new IllegalArgumentException(
                    String.format("Attribute [%s] is not supported.", attribute));
        }
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        FileStatus fileStatus = unwrapHdfsExceptions(() -> getHdfs().getFileStatus(hdfsPath));
        switch (attribute) {
            case SupportedFileAttributes.LAST_MODIFIED_TIME:
                return fileStatus.getModificationTime();
            case SupportedFileAttributes.SIZE:
                return fileStatus.getBlockSize();
            default:
                return null;
        }
    }

    @Override
    public boolean isDirectory(Path path)
    {
        org.apache.hadoop.fs.Path hdfsPath = toHdfsPath(path);
        try {
            return unwrapHdfsExceptions(() -> getHdfs().getFileStatus(hdfsPath).isDirectory());
        }
        catch (IOException e) {
            return false;
        }
    }

    @Override
    public Stream<Path> list(Path dir)
            throws IOException
    {
        validate(dir);
        FileStatus[] files = unwrapHdfsExceptions(() -> getHdfs().listStatus(toHdfsPath(dir)));
        Stream<FileStatus> sfiles = Stream.of(files);
        return sfiles.map(fileStatus -> {
            URI uriObj = URI.create(fileStatus.getPath().toString());
            return Paths.get(uriObj.getPath());
        });
    }

    @Override
    public Stream<Path> walk(Path dir)
            throws IOException
    {
        validate(dir);
        try {
            Stream<Path> children = list(dir).flatMap(path -> {
                try {
                    if (isDirectory(path)) {
                        return walk(path);
                    }
                    else {
                        return Stream.of(path);
                    }
                }
                catch (IOException e) {
                    // Java's flatMap does not support checked exceptions
                    // Wrapped it in RuntimeException and catch outside
                    throw new RuntimeException(e);
                }
            });
            return isDirectory(dir) ? Stream.concat(Stream.of(dir), children) : children;
        }
        catch (RuntimeException re) {
            if (re.getCause() instanceof IOException) {
                throw new IOException(re.getCause().getMessage());
            }
            else {
                throw re;
            }
        }
        catch (FileNotFoundException e) {
            throw new NoSuchFileException(e.getMessage());
        }
    }

    @Override
    public void close()
            throws IOException
    {
        getHdfs().close();
    }

    /**
     * Getter for filesystem object (lazy instantiation)
     *
     * @return filesystem object configured through the Hadoop configuration object
     */
    public FileSystem getHdfs()
    {
        return hdfs;
    }

    // Utility functions and interfaces below

    private void checkFileExists(org.apache.hadoop.fs.Path path)
            throws IOException
    {
        try {
            getHdfs().getFileStatus(path);
        }
        catch (FileNotFoundException e) {
            throw new NoSuchFileException(e.getMessage());
        }
    }

    private <T> T unwrapHdfsExceptions(ProcedureThrowingRemoteException<T> procedure)
            throws IOException
    {
        return unwrapHdfsExceptions(procedure, false);
    }

    private <T> T unwrapHdfsExceptions(ProcedureThrowingRemoteException<T> procedure, boolean recursive)
            throws IOException
    {
        try {
            try {
                return procedure.invoke();
            }
            catch (RemoteException re) {
                throw re.unwrapRemoteException(FileNotFoundException.class,
                        AlreadyBeingCreatedException.class);
            }
        }
        catch (FileNotFoundException e) {
            throw new NoSuchFileException(Throwables.getStackTraceAsString(e));
        }
        catch (org.apache.hadoop.fs.FileAlreadyExistsException | AlreadyBeingCreatedException e) {
            throw new FileAlreadyExistsException(Throwables.getStackTraceAsString(e));
        }
        catch (PathIsNotEmptyDirectoryException e) {
            throw new DirectoryNotEmptyException(Throwables.getStackTraceAsString(e));
        }
        catch (IOException e) {
            if (e.getMessage().matches(ERROR_MSG_NON_EMPTY_FOR_LOCAL_USE)) {
                // If hdfs is used locally, an IOException will be thrown by RawLocalFileSystem
                // instead of PathIsNotEmptyDirectoryException. Check error message instead.
                throw new DirectoryNotEmptyException(Throwables.getStackTraceAsString(e));
            }
            else if (e.getMessage().contains(ERROR_MSG_INITIATED_FAILED) && !recursive) {
                // hdfs client expired, create a new one and retry
                LOG.info("HDFS filesystem client expired, renewing a new one");
                this.hdfs = FileSystem.get(hdfsConfig);
                return unwrapHdfsExceptions(procedure, true);
            }
            else {
                throw e;
            }
        }
    }

    private interface ProcedureThrowingRemoteException<T>
    {
        T invoke()
                throws IOException;
    }
}
