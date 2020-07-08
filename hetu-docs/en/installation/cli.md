+++

weight = 3
title = "Command Line Interface"
+++

# Command Line Interface

The openLooKeng CLI provides a terminal-based interactive shell for running queries. The CLI is a [self-executing](http://skife.org/java/unix/2011/06/20/really_executable_jars.html) JAR file, which means it acts like a normal UNIX executable.

Download `hetu-cli-010-executable.jar`, rename it to `openlk-cli`, make
it executable with `chmod +x`, then run it:

``` shell
./openlk-cli --server localhost:8080 --catalog hive --schema default
```

Run the CLI with the `--help` option to see the available options.

By default, the results of queries are paginated using the `less` program which is configured with a carefully selected set of options. This behavior can be overridden by setting the environment variable `OPENLOOKENG_PAGER` to the name of a different program such as `more`, or set it to an empty value to completely disable pagination.
