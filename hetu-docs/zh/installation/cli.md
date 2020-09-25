
# 命令行接口

openLooKeng CLI提供了一个基于终端的交互shell，用于运行查询。CLI是一个可执行的JAR文件，可以通过`java -jar ./hetu-cli-*.jar`执行。

下载于服务器对应版本的 CLI 文件，例如：`hetu-cli-1.0.0-executable.jar`，运行：

``` shell
java -jar ./hetu-cli-1.0.0-executable.jar --server localhost:8080 --catalog hive --schema default
```

使用`--help`选项运行CLI，查看可用选项。

默认情况下，使用`less`程序将查询结果分页，该程序配置了一组精心选择的选项。可以通过将环境变量`OPENLOOKENG_PAGER`设置为其他程序的名称（如`more`）或将其设置为空值来完全禁用分页来覆盖此行为。