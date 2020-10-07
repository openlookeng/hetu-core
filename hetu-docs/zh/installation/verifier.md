

# openLooKeng验证器

openLooKeng验证器可用于针对另一个数据库（如MySQL）测试openLooKeng，或针对彼此测试两个openLooKeng集群。在开发openLooKeng时，我们使用它来针对先前的发行版持续测试主干。使用下表创建MySQL数据库，并通过你想要运行的查询加载该数据库：

``` sql
CREATE TABLE verifier_queries(
    id INT NOT NULL AUTO_INCREMENT,
    suite VARCHAR(256) NOT NULL,
    name VARCHAR(256),
    test_catalog VARCHAR(256) NOT NULL,
    test_schema VARCHAR(256) NOT NULL,
    test_prequeries TEXT,
    test_query TEXT NOT NULL,
    test_postqueries TEXT,
    test_username VARCHAR(256) NOT NULL default 'verifier-test',
    test_password VARCHAR(256),
    control_catalog VARCHAR(256) NOT NULL,
    control_schema VARCHAR(256) NOT NULL,
    control_prequeries TEXT,
    control_query TEXT NOT NULL,
    control_postqueries TEXT,
    control_username VARCHAR(256) NOT NULL default 'verifier-test',
    control_password VARCHAR(256),
    session_properties_json VARCHAR(2048),
    PRIMARY KEY (id)
);
```

接下来，创建一个属性文件来配置验证器：

``` properties
suite=my_suite
query-database=jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password
control.gateway=jdbc:lk://localhost:8080
test.gateway=jdbc:lk://localhost:8081
thread-count=1
```

最后，从[Maven Central](https://repo1.maven.org/maven2/io/hetu/core/presto-benchmark-driver/)下载合适版本的验证器可执行Jar文件，例如：[presto-verifier-1.0.1-executable.jar](https://repo1.maven.org/maven2/io/hetu/core/presto-verifier/1.0.1/presto-verifier-1.0.1-executable.jar)，重命名为`verifier`后，通过`chmod +x`使其可执行，然后运行：

``` shell
./verifier verify config.properties
```

如果预期版本不存在，可以使用`1.0.1`。