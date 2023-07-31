package io.hetu.core.filesystem.db;

import io.airlift.log.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DbUtils {

    private static final Logger LOG = Logger.get(DbUtils.class);

    private static final String SELECT_ALL_SQL = "select * from olk_fs_catalog";
    private static final String SELECT_ONE_SQL = "select * from olk_fs_catalog where catalog_name = ?";
    private static final String INSERT_SQL = "insert into olk_fs_catalog (catalog_name, metadata, properties, create_time) values(?,?,?,?)";
    private static final String UPDATE_SQL = "update olk_fs_catalog set metadata = ?, properties = ? where catalog_name = ?";
    private static final String DELETE_SQL = "delete from olk_fs_catalog where catalog_name = ?";


    public static List<DbCatalog> selectAll(DataSource dataSource) {
        List<DbCatalog> results = new ArrayList<>();
        execute(dataSource, SELECT_ALL_SQL, (p) -> {
            try {
                ResultSet resultSet = p.executeQuery();
                while (resultSet.next()) {
                    String catalogName = resultSet.getString("catalog_name");
                    String metadata = resultSet.getString("metadata");
                    String properties = resultSet.getString("properties");
                    results.add(new DbCatalog(catalogName, metadata, properties));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return results;
    }

    public static DbCatalog selectOne(DataSource dataSource, String catalog) {
        List<DbCatalog> results = new ArrayList<>();
        execute(dataSource, SELECT_ONE_SQL, (p) -> {
            try {
                p.setString(1, catalog);
                ResultSet resultSet = p.executeQuery();
                while (resultSet.next()) {
                    String catalogName = resultSet.getString("catalog_name");
                    String metadata = resultSet.getString("metadata");
                    String properties = resultSet.getString("properties");
                    results.add(new DbCatalog(catalogName, metadata, properties));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        if (!results.isEmpty()) {
            return results.get(0);
        }
        return null;
    }

    public static void insert(DataSource dataSource, String catalogName, String metadata, String properties) {
        execute(dataSource, INSERT_SQL, (p) -> {
            try {
                p.setString(1, catalogName);
                p.setString(2, metadata);
                p.setString(3, properties);
                p.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                p.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void updateByCatalogName(DataSource dataSource, String catalogName, String metadata, String properties) {
        execute(dataSource, UPDATE_SQL, (p) -> {
            try {
                p.setString(1, metadata);
                p.setString(2, properties);
                p.setString(3, catalogName);
                p.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void deleteByCatalogName(DataSource dataSource, String catalogName) {
        execute(dataSource, DELETE_SQL, (p) -> {
            try {
                p.setString(1, catalogName);
                p.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void execute(DataSource dataSource, String sql, Consumer<PreparedStatement> consumer) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            consumer.accept(preparedStatement);
        } catch (Exception e) {
            LOG.error("执行动态目录数据库语句异常, 原因: %s, %s", e.getMessage(), e);
            throw new RuntimeException("执行动态目录数据库语句异常, 原因: " + e.getMessage());
        } finally {
            closeConn(resultSet, preparedStatement, connection);
        }
    }

    private static void closeConn(ResultSet resultSet, Statement statement, Connection connection) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
