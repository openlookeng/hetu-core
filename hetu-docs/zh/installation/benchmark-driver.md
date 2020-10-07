
# 基准驱动

基准驱动可用于测量openLooKeng集群中查询的性能。我们用它来连续测量主干的性能。

从[Maven Central](https://repo1.maven.org/maven2/io/hetu/core/presto-benchmark-driver/)下载合适版本的基准驱动可执行Jar文件，例如[presto-benchmark-driver-1.0.1-executable.jar](https://repo1.maven.org/maven2/io/hetu/core/presto-benchmark-driver/1.0.1/presto-benchmark-driver-1.0.1-executable.jar)，重命名为`presto-benchmark-driver`后，使用`chmod +x`使其可执行。如果预期版本不存在，可以使用`1.0.1`。

## 套件

创建`suite.json`文件：

``` json
{
    "file_formats": {
        "query": ["single_.*", "tpch_.*"],
        "schema": [ "tpch_sf(?<scale>.*)_(?<format>.*)_(?<compression>.*?)" ],
        "session": {}
    },
    "legacy_orc": {
        "query": ["single_.*", "tpch_.*"],
        "schema": [ "tpch_sf(?<scale>.*)_(?<format>orc)_(?<compression>.*?)" ],
        "session": {
            "hive.optimized_reader_enabled": "false"
        }
    }
}
```

此示例包含两个套件`file_formats`和`legacy_orc`。`file_formats`套件将在与正则表达式`tpch_sf.*_.*_.*?`匹配的所有模式中运行名称与正则表达式`single_.*`或`tpch_.*`匹配的查询。`legacy_orc`套件添加了一个会话属性来禁用优化的ORC阅读器，并且只在`tpch_sf.*_orc_.*?`模式中运行。

## 查询

SQL文件包含在名为`sql`的目录中，并且必须具有`.sql`文件扩展名。查询的名称是不带扩展名的文件名。

## 输出

基准驱动将测量挂钟时间、所有openLooKeng进程使用的CPU总时间以及查询使用的CPU时间。对于每次计时，驱动报告查询运行的中位数、平均值和标准偏差。进程CPU时间和查询CPU时间之差是查询开销，这通常是垃圾收集器造成的。下面是上述`file_formats`套件的输出：

```
suite        query          compression format scale wallTimeP50 wallTimeMean wallTimeStd processCpuTimeP50 processCpuTimeMean processCpuTimeStd queryCpuTimeP50 queryCpuTimeMean queryCpuTimeStd
============ ============== =========== ====== ===== =========== ============ =========== ================= ================== ================= =============== ================ ===============
file_formats single_varchar none        orc    100   597         642          101         100840            97180              6373              98296           94610            6628
file_formats single_bigint  none        orc    100   238         242          12          33930             34050              697               32452           32417            460
file_formats single_varchar snappy      orc    100   530         525          14          99440             101320             7713              97317           99139            7682
file_formats single_bigint  snappy      orc    100   218         238          35          34650             34606              83                33198           33188            83
file_formats single_varchar zlib        orc    100   547         543          38          105680            103373             4038              103029          101021           3773
file_formats single_bigint  zlib        orc    100   282         269          23          38990             39030              282               37574           37496            156
```

请注意，上面的输出已重新格式化，以提高驱动输出的标准TSV的可读性。

驱动可以通过从模式名或SQL文件中提取值来向输出中添加额外的列。在上述套件文件中，架构名称包含`compression`、`format`和`scale`的命名正则表达式捕获组，因此如果我们在一个包含模式 `tpch_sf100_orc_none`、 `tpch_sf100_orc_snappy`、 和 `tpch_sf100_orc_zlib` 的目录中运行查询，我们得到上面的输出。

创建其他输出列的另一种方法是向SQL文件添加标记。例如，下面的SQL文件中声明了两个标签`projection`和`filter`：

```
projection=true
filter=false
=================
SELECT SUM(LENGTH(comment))
FROM lineitem
```

这将导致驱动为每次运行此查询输出这些值。

## CLI参数

`presto-benchmark-driver`程序包含许多CLI参数，用于控制运行哪个套件和查询、预热运行的数量和测量运行的数量。通过`--help`选项可以看到所有的命令行参数。