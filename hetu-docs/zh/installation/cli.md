+++

weight = 3
title = "命令行接口"
+++

# 命令行接口

openLooKeng CLI提供了一个基于终端的交互shell，用于运行查询。CLI是一个[自动执行](http://skife.org/java/unix/2011/06/20/really_executable_jars.html)的JAR文件，这意味着它就像一个普通的UNIX可执行文件。

下载：maven\_download:\[cli]{.title-ref}，重命名为`openlk-cli`后，通过`chmod +x`使其执行，然后运行：

```{.none}
./openlk-cli --server localhost:8080 --catalog hive --schema default
```

使用`--help`选项运行CLI，查看可用选项。

默认情况下，使用`less`程序将查询结果分页，该程序配置了一组精心选择的选项。可以通过将环境变量`OPENLOOKENG_PAGER`设置为其他程序的名称（如`more`）或将其设置为空值来完全禁用分页来覆盖此行为。