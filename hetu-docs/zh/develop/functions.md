
# 函数

## 插件实现

函数框架用于实现 SQL 函数。openLooKeng 包含大量的内置函数。
为了实现新的函数，您可以编写一个插件，用于从 ``getFunctions()`` 返回一个或多个函数：

``` java
public class ExampleFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ExampleNullFunction.class)
                .add(IsNullFunction.class)
                .add(IsEqualOrNullFunction.class)
                .add(ExampleStringFunction.class)
                .add(ExampleAverageFunction.class)
                .build();
    }
}
```

请注意，``ImmutableSet`` 类是 Guava 提供的一个实用程序类。
``getFunctions()`` 方法包含我们将在本教程的下面部分实现的所有函数类。


有关代码库的完整示例，请参见 ``presto-m`` 模块中的机器学习函数或 ``presto-teradata-functions`` 模块中与 Teradata 兼容的函数，这两个模块都位于在 openLooKeng 源代码的根目录中。



## 标量函数实现

函数框架使用注释来表示函数的相关信息，包括名称、说明、返回类型和参数类型。

下面是一个实现 ``is_null`` 的示例函数：

``` java
public class ExampleNullFunction
{
    @ScalarFunction("is_null")
    @Description("Returns TRUE if the argument is NULL")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNull(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        return (string == null);
    }
}
```

函数 ``is_null`` 接受单个 ``VARCHAR`` 参数并返回一个 ``BOOLEAN`` 值，指示该参数是否为 ``NULL``。
请注意，该函数的参数类型是 ``slice``。
``VARCHAR`` 使用 ``Slice``，后者本质上是一个对 ``byte[]`` 进行包装的包装器，而不是用于其本地容器类型的 ``String``。


* ``@SqlType``：

  ``@SqlType`` 注释用于声明返回类型和参数类型。
  请注意，Java 代码的返回类型和参数必须与相应注释的本地容器类型相匹配。
  
* ``@SqlNullable``：

  ``@SqlNullable`` 注释表示参数可能是 ``NULL``。如果没有该注释，框架会在函数的任一参数是 ``NULL`` 时假定函数返回 ``NULL``。
  
  在使用具有基元本地容器类型（如 ``BigintType``）的 ``Type`` 时，如果使用 ``@SqlNullable``，请使用本地容器类型的对象包装器。
  
  如果方法在参数非空时可以返回 ``NULL``，则必须使用 ``@SqlNullable`` 对其进行注释。
  

## 参数化标量函数

具有类型参数的标量函数更加复杂一些。
要使前面的示例适用于任何类型，我们需要添加以下代码：

``` java
@ScalarFunction(name = "is_null")
@Description("Returns TRUE if the argument is NULL")
public final class IsNullFunction
{
    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullSlice(@SqlNullable @SqlType("T") Slice value)
    {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullLong(@SqlNullable @SqlType("T") Long value)
    {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullDouble(@SqlNullable @SqlType("T") Double value)
    {
        return (value == null);
    }

    // ...and so on for each native container type
}
```

* ``@TypeParameter``：

  ``@TypeParameter`` 注释用于声明可用于参数类型 ``@SqlType`` 注释的类型参数或函数的返回类型。
  
  它还可以用于注释类型为 ``Type`` 的参数。在运行时，引擎会将具体类型绑定到此参数。
  可以使用 ``@OperatorDependency`` 来声明需要一个对给定的类型参数进行操作的附加函数。
  
  例如，以下函数将仅绑定到定义了 equals 函数的类型：
  
``` java
@ScalarFunction(name = "is_equal_or_null")
@Description("Returns TRUE if arguments are equal or both NULL")
public final class IsEqualOrNullFunction
{
    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isEqualOrNullSlice(
            @OperatorDependency(operator = OperatorType.EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlNullable @SqlType("T") Slice value1,
            @SqlNullable @SqlType("T") Slice value2)
    {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }
        return (boolean) equals.invokeExact(value1, value2);
    }

    // ...and so on for each native container type
}
```

## 另一个标量函数示例

``lowercaser`` 函数接受单个 ``VARCHAR`` 参数并返回一个 ``VARCHAR`` 值，这是该参数转换为小写字母后的结果：


``` java
public class ExampleStringFunction
{
    @ScalarFunction("lowercaser")
    @Description("converts the string to alternating case")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice lowercaser(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(argument.toLowerCase());
    }
}
```

请注意，对于大多数常见的字符串函数（包括将字符串转换为小写的函数），Slice 库还提供了直接对基础 ``byte[]`` 进行操作的实现，这些实现的性能要好得多。

该函数没有 ``@SqlNullable`` 注释，这意味着如果参数为 ``NULL``，结果将自动为 ``NULL``（不会调用该函数）。



## 聚合函数实现

聚合函数使用与标量函数类似的框架，但稍微复杂一些。


* ``AccumulatorState``：

  所有聚合函数都将输入行累积到一个状态对象中；该对象必须实现 ``AccumulatorState``。
  对于简单的聚合，只需使用所需的获取器和设置器将 ``AccumulatorState`` 扩展为一个新的接口，框架将为您生成所有实现和序列化器。
  
  
  如果您需要更复杂的状态对象，则需要实现 ``AccumulatorStateFactory`` 和 ``AccumulatorStateSerializer`` 并通过 ``AccumulatorStateMetadata`` 注释来提供它们。
  
  

以下代码实现了用于计算 ``DOUBLE`` 列的平均值的聚合函数 ``avg_double``：


``` java
@AggregationFunction("avg_double")
public class AverageAggregation
{
    @InputFunction
    public static void input(LongAndDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @CombineFunction
    public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            DOUBLE.writeDouble(out, value / count);
        }
    }
}
```

该平均值由两部分组成：该列的每一行中的 ``DOUBLE`` 值之和与所见行的总数 (``LONG``)。
``LongAndDoubleState`` 是一个对 ``AccumulatorState`` 进行扩展的接口：


``` java
public interface LongAndDoubleState
        extends AccumulatorState
{
    long getLong();

    void setLong(long value);

    double getDouble();

    void setDouble(double value);
}
```

如上所述，对于简单的 ``AccumulatorState`` 对象，只需要定义包含获取器和设置器的接口，框架就可以为您生成实现。



下面是对与编写聚合函数相关的各种注释的深入介绍：


* ``@InputFunction``：

  ``@InputFunction`` 注释声明接受输入行并将其存储在 ``AccumulatorState`` 中的函数。
  与标量函数类似，您必须使用 ``@SqlType`` 对参数进行注释。
    请注意，与上面标量示例中 ``Slice`` 用于保存 ``VARCHAR`` 不同，基元 ``double`` 类型用于参数输入。
  
  在该示例中，输入函数仅跟踪正在运行的行总数（通过 ``setLong()`` 实现）以及正在运行的和（通过 ``setDouble(）`` 实现）。
  
  

* ``@CombineFunction``：

  ``@CombineFunction`` 注释声明用于组合两个状态对象的函数。
  该函数用于合并所有部分聚合状态。
  该函数接受两个状态对象，并将结果合并到第一个状态对象中（在上面的示例中，仅将两者相加）。
  
* ``@OutputFunction``：

  ``@OutputFunction`` 是在计算聚合时调用的最后一个函数。
  该函数接受最后一个状态对象（所有部分状态的合并结果）并将结果写入到 ``BlockBuilder`` 中。
  
* 序列化在何处发生，什么是 ``GroupedAccumulatorState``？

  ``@InputFunction`` 和 ``@CombineFunction`` 通常运行在不同的工作节点上，因此聚合框架会对状态对象进行序列化并在这些工作节点之间传递状态对象。
  
  在执行 ``GROUP BY`` 聚合时会使用 ``GroupedAccumulatorState``，如果您不指定 ``AccumulatorStateFactory``，系统会自动为您生成实现。
  
  
  
