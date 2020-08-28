
# 类型

openLooKeng 中的 ``Type`` 接口用于实现 SQL 语言中的类型。openLooKeng 具有大量的内置类型，如 ``VarcharType`` 和 ``BigintType``。``ParametricType`` 接口用于为类型提供类型参数，以允许实现 ``VARCHAR(10)`` 和 ``DECIMAL(22, 5)`` 等类型。``Plugin`` 可以通过从 ``getTypes()`` 返回 ``Type`` 对象来提供新的 ``Type`` 对象，并可以通过从 ``getParametricTypes()`` 返回 ``ParametricType`` 对象来提供新的 ``ParametricType`` 对象。

下面是 ``Type`` 接口的简要概述，有关更多详细信息，请参见 JavaDoc 中的 ``Type``。


* 本地容器类型：

  所有类型都定义了 ``getJavaType()`` 方法，通常称为“原生容器类型”。这是用于在执行过程中保存值并将其存储在 ``Block`` 中的 Java 类型。例如，这是 Java 代码中用于实现生产或消费该 ``Type`` 的函数的类型。
  
* 本地编码：

  对本地容器类型形式的值的解释由其 ``Type`` 进行定义。``BigintType`` 等某些类型匹配本地容器类型的 Java 解释（64 位补码）。不过，对于 ``TimestampWithTimeZoneType``（该类型还使用 ``long`` 作为其本地容器类型）等其他类型，存储在 ``long`` 中的值是一个 8 字节二进制值，该值将时区和 unix 纪元时间以来的毫秒数组合在一起。尤其要注意的是，这意味着在不知道本地编码的情况下，无法比较两个本地值并期望得到有意义的结果。
  
  
  
* 类型签名：

  类型的签名定义其标识，并对类型参数（如果类型是参数化的）和字面参数等关于该类型的一些一般信息进行编码。字面参数用于 ``VARCHAR(10)`` 等类型。
  
