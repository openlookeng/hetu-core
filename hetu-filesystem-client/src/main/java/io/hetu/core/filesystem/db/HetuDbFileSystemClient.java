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
package io.hetu.core.filesystem.db;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import io.airlift.log.Logger;
import io.hetu.core.filesystem.AbstractWorkspaceFileSystemClient;
import io.prestosql.spi.filesystem.SupportedFileAttributes;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.file.Files.getFileStore;
import static java.nio.file.Files.newDirectoryStream;

/**
 * 动态目录数据库客户端
 */
public class HetuDbFileSystemClient extends AbstractWorkspaceFileSystemClient {

    private static final Logger LOG = Logger.get(HetuDbFileSystemClient.class);

    private DataSource dataSource;

    private List<String> catalogBaseDir = new ArrayList<>();

    public HetuDbFileSystemClient(DbConfig config, Path allowAccessRoot) {
        super(allowAccessRoot);
        try {
            dataSource = DruidDataSourceFactory.createDataSource(config.getDbProperties());
        } catch (Exception e) {
            LOG.error("获取动态目录数据库配置异常, 原因: {}", e.getMessage(), e);
            throw new IllegalArgumentException("获取数据库动态目录配置异常, 原因: " + e.getMessage());
        }
    }

    @Override
    public Path createDirectories(Path dir)
            throws IOException {
        validate(dir);
        return Files.createDirectories(dir);
    }

    @Override
    public Path createDirectory(Path dir)
            throws IOException {
        validate(dir);
        return Files.createDirectory(dir);
    }

    /**
     * Delete a given file or directory. If the given path is a directory it must be empty.
     *
     * @param path Path to delete.
     * @throws IOException Other exceptions.
     */
    @Override
    public void delete(Path path)
            throws IOException {
        validate(path);
        Files.delete(path);
    }

    /**
     * Delete a given file or directory. If the given path is a directory it must be empty.
     * Return the result of deletion.
     *
     * @param path Path to delete.
     * @return Whether the deletion is successful. If the file does not exist, return {@code false}.
     * @throws IOException Other exceptions.
     */
    public boolean deleteIfExistsNoDb(Path path) throws IOException {
        validate(path);
        return Files.deleteIfExists(path);
    }

    public boolean deleteRecursivelyNoDb(Path path)
            throws FileSystemException {
        validate(path);
        if (!exists(path)) {
            return false;
        }
        Collection<IOException> exceptions = new LinkedList<>();
        deleteRecursivelyCore(path, exceptions);
        if (!exceptions.isEmpty()) {
            FileSystemException exceptionToThrow = new FileSystemException(path.toString(), null,
                    "Failed to delete one or more files. Please checked suppressed exceptions for details");
            for (IOException ex : exceptions) {
                exceptionToThrow.addSuppressed(ex);
            }
            throw exceptionToThrow;
        }
        return true;
    }

    @Override
    public boolean deleteIfExists(Path path)
            throws IOException {
        // 删除连接器
        deleteDbCatalog(path);
        // localfile正常流程
        validate(path);
        return Files.deleteIfExists(path);
    }

    @Override
    public boolean deleteRecursively(Path path)
            throws FileSystemException {
        // 删除连接器
        deleteDbCatalog(path);
        // localfile正常流程
        validate(path);
        if (!exists(path)) {
            return false;
        }
        Collection<IOException> exceptions = new LinkedList<>();
        deleteRecursivelyCore(path, exceptions);
        if (!exceptions.isEmpty()) {
            FileSystemException exceptionToThrow = new FileSystemException(path.toString(), null,
                    "Failed to delete one or more files. Please checked suppressed exceptions for details");
            for (IOException ex : exceptions) {
                exceptionToThrow.addSuppressed(ex);
            }
            throw exceptionToThrow;
        }
        return true;
    }

    private void deleteRecursivelyCore(Path path, Collection<IOException> exceptions) {
        if (!exists(path)) {
            exceptions.add(new FileNotFoundException(path.toString()));
            return;
        }
        if (!Files.isDirectory(path)) {
            try {
                delete(path);
            } catch (IOException ex) {
                exceptions.add(ex);
            }
        } else {
            try (Stream<Path> children = list(path)) {
                if (children != null) {
                    children.forEach(child -> deleteRecursivelyCore(child, exceptions));
                }
                delete(path);
            } catch (IOException ex) {
                exceptions.add(ex);
            }
        }
    }

    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }

    @Override
    public void move(Path source, Path target)
            throws IOException {
        validate(source);
        validate(target);
        Files.move(source, target);
    }

    @Override
    public InputStream newInputStream(Path path)
            throws IOException {
        // Need inline check to pass security check
        validate(path);
        return Files.newInputStream(path);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options)
            throws IOException {
        // Need inline check to pass security check
        validate(path);
        return new DbFileOutputStream(dataSource, catalogBaseDir, path, Files.newOutputStream(path, options));
    }

    @Override
    public Object getAttribute(Path path, String attribute)
            throws IOException {
        validate(path);
        if (!SupportedFileAttributes.SUPPORTED_ATTRIBUTES.contains(attribute)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Attribute [%s] is not supported.", attribute));
        }
        // Get time in millis instead of date time format
        if (attribute.equalsIgnoreCase(SupportedFileAttributes.LAST_MODIFIED_TIME)) {
            return Files.getLastModifiedTime(path).toMillis();
        }
        return Files.getAttribute(path, attribute);
    }

    @Override
    public boolean isDirectory(Path path) {
        return Files.isDirectory(path);
    }

    @Override
    public Stream<Path> list(Path dir)
            throws IOException {
        // 同步数据库连接器
        syncDbCatalog(dir);
        // localfile正常流程
        validate(dir);
        return Files.list(dir);
    }

    @Override
    public Stream<Path> walk(Path dir)
            throws IOException {
        validate(dir);
        return Files.walk(dir);
    }

    @Override
    public void close() {
    }

    @Override
    public long getTotalSpace(Path path) throws IOException {
        FileStore fileStore = getFileStore(path);
        return fileStore.getTotalSpace();
    }

    @Override
    public long getUsableSpace(Path path) throws IOException {
        FileStore fileStore = getFileStore(path);
        return fileStore.getUsableSpace();
    }

    @Override
    public Path createTemporaryFile(Path path, String prefix, String suffix) throws IOException {
        return Files.createTempFile(path, prefix, suffix);
    }

    @Override
    public Path createFile(Path path) throws IOException {
        return Files.createFile(path);
    }

    @Override
    public Stream<Path> getDirectoryStream(Path path, String prefix, String suffix) throws IOException {
        String glob = prefix + "*" + suffix;
        return StreamSupport.stream(newDirectoryStream(path, glob).spliterator(), false);
    }

    /**
     * 同步数据库连接器
     *
     * @param dir
     * @throws Exception
     */
    private void syncDbCatalog(Path dir) throws IOException {
        if (dir.toFile().getName().equals("catalog")) {
            String absolutePath = dir.toFile().getAbsolutePath();
            // 保存动态目录的根目录
            if (!catalogBaseDir.contains(absolutePath)) {
                catalogBaseDir.add(absolutePath);
            }
            // 获取数据源
            Map<String, DbCatalog> dbCatalogs = DbUtils.selectAll(dataSource).stream().collect(Collectors.toMap(DbCatalog::getCatalogName, o -> o));
            // 获取本地文件
            Map<String, DbCatalog> localCatalogs = getLocalCatalog(dir);
            // 删除本地文件
            for (String localKey : localCatalogs.keySet()) {
                DbCatalog dbCatalog = dbCatalogs.get(localKey);
                DbCatalog localCatalog = localCatalogs.get(localKey);
                if (dbCatalog == null) {
                    if (localCatalog.getPropertiesPath() != null) {
                        deleteIfExistsNoDb(localCatalog.getPropertiesPath().toAbsolutePath());
                    }
                    if (localCatalog.getMetadataDirPath() != null) {
                        deleteRecursivelyNoDb(localCatalog.getMetadataDirPath().toAbsolutePath());
                    }
                }
            }
            // 增加本地文件
            saveLocalFile(absolutePath, dbCatalogs, localCatalogs);
        }
    }

    /**
     * 获取本地配置
     *
     * @param dir
     * @return
     * @throws IOException
     */
    private Map<String, DbCatalog> getLocalCatalog(Path dir) throws IOException {
        Map<String, DbCatalog> localCatalogs = new HashMap<>();
        List<Path> propertiesPath = Files.list(dir).map(Path::getFileName).collect(Collectors.toList());
        if (!propertiesPath.isEmpty()) {
            for (Path path : propertiesPath) {
                String fileName = path.toString();
                if (fileName.endsWith(".properties")) {
                    String catalogName = fileName.substring(0, fileName.lastIndexOf(".properties"));
                    Path propertiesAPath = Paths.get(dir.toString(), path.toString()).toAbsolutePath();
                    localCatalogs.put(catalogName, new DbCatalog(catalogName, null, null, readString(Files.newInputStream(propertiesAPath)), propertiesAPath));
                }
            }
        }
        List<File> metadataDirPath = Files.list(dir).map(Path::toFile).filter(File::isDirectory).collect(Collectors.toList());
        if (!metadataDirPath.isEmpty()) {
            for (File path : metadataDirPath) {
                File metadataPath = Files.list(path.toPath()).map(Path::toFile).filter(o -> o.getName().endsWith(".metadata")).findFirst().orElse(null);
                if (metadataPath != null) {
                    // 获取metadata文件加metadata目录
                    String fileName = metadataPath.getName();
                    DbCatalog dbCatalog = localCatalogs.get(fileName.substring(0, fileName.lastIndexOf(".metadata")));
                    if (dbCatalog != null) {
                        dbCatalog.setMetadata(readString(Files.newInputStream(Paths.get(metadataPath.getAbsolutePath()).toAbsolutePath())));
                        dbCatalog.setMetadataDirPath(path.toPath());
                    }
                } else {
                    // 获取metadata目录
                    DbCatalog dbCatalog = localCatalogs.get(path.getName());
                    if (dbCatalog != null) {
                        dbCatalog.setMetadataDirPath(path.toPath());
                    }
                }
            }
        }
        return localCatalogs;
    }

    /**
     * 保存本地文件
     *
     * @param absolutePath
     * @param dbCatalogs
     * @param localCatalogs
     * @throws IOException
     */
    private void saveLocalFile(String absolutePath, Map<String, DbCatalog> dbCatalogs, Map<String, DbCatalog> localCatalogs) throws IOException {
        // 增加本地文件
        for (String dbKey : dbCatalogs.keySet()) {
            DbCatalog dbCatalog = dbCatalogs.get(dbKey);
            DbCatalog localCatalog = localCatalogs.get(dbKey);
            if (localCatalog != null) {
                if (!dbCatalog.getMetadata().equals(localCatalog.getMetadata())
                        || !dbCatalog.getProperties().equals(localCatalog.getProperties())) {
                    if (localCatalog.getPropertiesPath() != null) {
                        deleteIfExistsNoDb(localCatalog.getPropertiesPath().toAbsolutePath());
                    }
                    if (localCatalog.getMetadataDirPath() != null) {
                        deleteRecursivelyNoDb(localCatalog.getMetadataDirPath().toAbsolutePath());
                    }
                }
            }
            // 创建properties
            Path catalogPropertiesPath = Paths.get(absolutePath, dbCatalog.getCatalogName() + ".properties");
            Properties catalogProperties = new Properties();
            catalogProperties.load(new StringReader(dbCatalog.getProperties()));
            catalogProperties.store(Files.newOutputStream(catalogPropertiesPath), "创建连接器[" + dbCatalog.getCatalogName() + "]的properties文件");
            // 创建目录
            Path catalogDir = Paths.get(absolutePath, dbCatalog.getCatalogName());
            if (!Files.exists(catalogDir)) {
                Files.createDirectory(catalogDir);
            }
            // 创建metadata
            Path metadataPropertiesPath = Paths.get(absolutePath, dbCatalog.getCatalogName(), dbCatalog.getCatalogName() + ".metadata");
            Properties metadataProperties = new Properties();
            metadataProperties.load(new StringReader(dbCatalog.getMetadata()));
            metadataProperties.store(Files.newOutputStream(metadataPropertiesPath), "创建连接器[" + dbCatalog.getCatalogName() + "]的metadata文件");
        }
    }

    /**
     * 删除数据库连接器
     *
     * @param path
     * @throws Exception
     */
    private void deleteDbCatalog(Path path) {
        // 添加/更新连接器
        String absolutePath = path.toFile().getParentFile().getAbsolutePath();
        if (!catalogBaseDir.isEmpty() && catalogBaseDir.stream().filter(absolutePath::contains).count() > 0) {
            String fileName = path.toFile().getName();
            String catalogName = null;
            if (fileName.endsWith(".properties")) {
                catalogName = fileName.substring(0, fileName.lastIndexOf(".properties"));
            }
            if (fileName.endsWith(".metadata")) {
                catalogName = fileName.substring(0, fileName.lastIndexOf(".metadata"));
            }
            if (catalogName != null) {
                DbCatalog dbCatalog = DbUtils.selectOne(dataSource, catalogName);
                if (dbCatalog != null) {
                    DbUtils.deleteByCatalogName(dataSource, dbCatalog.getCatalogName());
                }
            }
        }
    }

    /**
     * 读取文件内容字符串
     *
     * @param stream
     * @return
     */
    public static String readString(InputStream stream) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, Charset.defaultCharset()))) {
            String s = null;
            while ((s = br.readLine()) != null) {
                sb.append(s);
                sb.append("\r\n");
            }
            br.close();
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
