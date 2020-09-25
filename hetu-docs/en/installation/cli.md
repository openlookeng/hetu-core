
# Command Line Interface

The openLooKeng CLI provides a terminal-based interactive shell for running queries. The CLI is a runnable JAR file, so it can be run as `java -jar ./hetu-cli-*.jar`.

Download CLI jar file corresponding to the server's version, e.g. `hetu-cli-1.0.0-executable.jar`, and run:

``` shell
java -jar ./hetu-cli-1.0.0-executable.jar --server localhost:8080 --catalog hive --schema default
```

Run the CLI with the `--help` option to see the available options.

By default, the results of queries are paginated using the `less` program which is configured with a carefully selected set of options. This behavior can be overridden by setting the environment variable `OPENLOOKENG_PAGER` to the name of a different program such as `more`, or set it to an empty value to completely disable pagination.
