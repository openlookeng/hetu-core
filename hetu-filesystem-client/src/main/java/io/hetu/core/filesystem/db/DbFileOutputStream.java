package io.hetu.core.filesystem.db;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.hetu.core.filesystem.db.HetuDbFileSystemClient.readString;

public class DbFileOutputStream extends OutputStream {

    private DataSource dataSource;

    private List<String> catalogBaseDir;

    private Path path;

    private OutputStream out;

    public DbFileOutputStream(DataSource dataSource, List<String> catalogBaseDir, Path path, OutputStream out) {
        this.dataSource = dataSource;
        this.catalogBaseDir = catalogBaseDir;
        this.path = path;
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
        saveDbCatalog(path);
    }

    /**
     * 保存数据库连接器
     * @param path
     * @throws Exception
     */
    private void saveDbCatalog(Path path) throws IOException {
        String absolutePath = path.toFile().getParentFile().getAbsolutePath();
        if (!catalogBaseDir.isEmpty() && catalogBaseDir.stream().filter(absolutePath::contains).count() > 0) {
            String fileName = path.toFile().getName();
            String catalogName = null;
            String metatdata = null;
            String properties = null;
            if (fileName.endsWith(".properties") && Files.exists(path.toAbsolutePath())) {
                catalogName = fileName.substring(0, fileName.lastIndexOf(".properties"));
                properties = readString(Files.newInputStream(path.toAbsolutePath()));
            }
            if (fileName.endsWith(".metadata") && Files.exists(path.toAbsolutePath())) {
                catalogName = fileName.substring(0, fileName.lastIndexOf(".metadata"));
                metatdata = readString(Files.newInputStream(path.toAbsolutePath()));
            }
            if (catalogName != null) {
                DbCatalog dbCatalog = DbUtils.selectOne(dataSource, catalogName);
                if (dbCatalog != null) {
                    if (metatdata != null) {
                        dbCatalog.setMetadata(metatdata);
                    }
                    if (properties != null) {
                        dbCatalog.setProperties(properties);
                    }
                    DbUtils.updateByCatalogName(dataSource, dbCatalog.getCatalogName(), dbCatalog.getMetadata(), dbCatalog.getProperties());
                } else {
                    DbUtils.insert(dataSource, catalogName, metatdata, properties);
                }
            }
        }
    }
}
