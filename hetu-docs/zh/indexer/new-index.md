# 创建自定义索引

## 基本概念

### ID

每一个索引类型都必须有一个`ID`变量作为其唯一标识符。这一名称将用于CLI，服务器配置，存储索引的文件路径中等各种场合，用于唯一地确定这一索引类型。

`Index`接口的`getID()`方法会将索引的ID返回给它的使用者。

### Level （数据）层级

启发式索引用各种不同的方法，存储了在原始数据之外额外并且通常更小的信息，用于提高随机查询的效率。因此，每一条索引都必须有它作用的数据域范围。例如，如果一个索引
记录了数据集的"最大"数值，那这一"最大"数值必须拥有它作用的范围（可以是一组数据行，也可以是一个数据分区，甚至整张数据表的最大值）。当创建一个新的索引类型是，必须
实现`Set<Level> getSupportedIndexLevels();` 方法来告知它可以支持的数据层级。这些层级由`Index`接口中的一个枚举类型定义。

## 接口概览

### 索引方法

除了上面提到的方法以外，这一段落将介绍其他几个实现新索引类型最重要的方法。要获取`Index`接口的完整文档，请参阅源代码的Java Doc。

`Index`接口有两个最重要的方法：

```java
boolean matches(Object expression) throws UnsupportedOperationException;

<I> Iterator<I> lookUp(Object expression) throws UnsupportedOperationException;
```

第一个`matches()`方法接收一个表达式对象，并返回基于索引存储的信息，表达式对应的数据是否可能存在。例如，如果一个索引标记了数据中的最大值，那他可以容易地判断`col_val > 5`是否可能成立。

第二个方法`lookUp()`不是必须的。在返回数据是否可能存在以外，他还能返回一个迭代器来返回数据可能存在的位置。对于一个索引，它的`matches()`方法和`lookUp().hasNext()`应当总是返回相同的结果。

### 插入和存取索引

下面的几个方法用于向一个索引中添加值，以及在磁盘上存储/读取索引实例：

```java
boolean addValues(Map<String, List<Object>> values) throws IOException;

Index deserialize(InputStream in) throws IOException;

void serialize(OutputStream out) throws IOException;
``` 

这些方法的使用非常直观。为了更好地理解他们的用法，`MinMaxIndex`的源代码可以作为一个很好的例子。在这个索引中，添加数据仅仅需要通过新的数据更新`max`和`min`变量即可。
`serialize()/deserialize()`方法则只需要向/从磁盘写入/读取最大和最小值这两个值。