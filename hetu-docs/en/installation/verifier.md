
# openLooKeng Verifier

The openLooKeng Verifier can be used to test openLooKeng against another database (such as MySQL), or to test two openLooKeng clusters against each other. We use it to continuously test trunk against the previous release while developing openLooKeng. Create a MySQL database with the following table and load it with the queries you would like to run:

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

Next, create a properties file to configure the verifier:

``` properties
suite=my_suite
query-database=jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password
control.gateway=jdbc:lk://localhost:8080
test.gateway=jdbc:lk://localhost:8081
thread-count=1
```

Lastly, download the appropriate version of the verifier executable jar file from [Maven Central](https://repo1.maven.org/maven2/io/hetu/core/presto-benchmark-driver/), for example [presto-verifier-1.0.1-executable.jar](https://repo1.maven.org/maven2/io/hetu/core/presto-verifier/1.0.1/presto-verifier-1.0.1-executable.jar), rename it to `verifier`, make it executable with `chmod +x`, then run it:

``` shell
./verifier verify config.properties
```

If the specific version is not available, use `1.0.1` instead.