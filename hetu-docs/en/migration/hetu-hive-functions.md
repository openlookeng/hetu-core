###  Overall Design

In openLooKeng, we support dynamically loading user-defined Hive scalar functions. Basically openLooKeng will load metadata of Hive functions, register them, convert parameters of the `evaluate` methods from Hive internal data types to openLooKeng internal data types, and then dynamically generate the functions.

![image](../images/hetu-hive-functions.png)

### **Configuration**
1.  In order to dynamically load Hive functions, users should add function metadata into `udf.properties`, with the format: `function_name class_path`. An example configuration in `udf.properties` is presented as below:
 ```
 booleanudf io.hetu.core.hive.dynamicfunctions.examples.udf.BooleanUDF
 shortudf io.hetu.core.hive.dynamicfunctions.examples.udf.ShortUDF
 byteudf io.hetu.core.hive.dynamicfunctions.examples.udf.ByteUDF
 intudf io.hetu.core.hive.dynamicfunctions.examples.udf.IntUDF
 ```
2.  Users should upload Hive functions jars and **dependencies** onto a separate directory under `${node.data-dir}` which is setting in `node.properties`. The directory is user configurable by setting `external-functions.dir` and the default value is `externalFunctions`. An example configuration in `config.properties` is presented as below:
```
external-functions.dir=externalFunctions
``` 
so by default, user should upload their hive functions to the `externalFunctions` file folder.

3.  Users should upload Hive functions configuration files `udf.properties` into `${node.data-dir}/etc`.

### **Asynchronous execution**
  
Considering the system safety, we provide a mechanism to execute hive function asynchronously.

1. The user-defined Hive functions can be executed in another thread by setting `max-function-running-time-enable`.  The default value is `false`
2. Users can limit the maximum running time of hive functions by setting `max-function-running-time-in-second`. The default value is `600 seconds`.
3. Users can also limit the thread pool size of running the hive functions by setting `function-running-thread-pool-size`. The default value is `100`

An example configuration in `config.properties` is presented as below:
```
max-function-running-time-enable=true
max-function-running-time-in-second=300
function-running-thread-pool-size=10
```
**Attention: Since each row data of the table may use the hive function once, so enable the hive function asynchronous execution may lead to seriously performance degradation.Please choose a balance between security and performance according to the actual situation**

### **Details**
1.  In openLooKeng, we only support UDF with the following types:
```
boolean, byte, short, int, long, float, double
Boolean, Byte, Short, Int, Long, Float, Double
List, Map
```
2. Only functions with equals to or less than five parameters are supported. If users add functions with more than five parameters, openLooKeng will ignore the function and print error logs.
3.  If users add inaccurate function metadata into `udf.properties`, such as wrong formats or non-existing class paths, openLooKeng will ignore the metadata and print error logs.
1.  If users add duplicated function metadata into the properties file, openLooKeng will recognize and discard the duplicated ones.
1.  If user-defined functions have same signatures as the internal ones, openLooKeng will ignore user-defined ones and print error logs.
1.  Users can add functions with overloaded evaluate methods. openLooKeng will recognize all signatures and create functions for each signature.
1.  If users execute functions with null parameters, the system will directly return null instead of parsing the null values to the function.

* **Notes for UDAF**

Currently we don't support loading user defined Hive UDAFs. But user can still use their UDAF functions which are developed by the openLooKeng's [function framework](https://openlookeng.io/zh-cn/docs/docs/develop/functions.html) under this feature to use the asynchronous execution mechanism.User can copy the functions and dependencies into a directory under `${node.data-dir}` dir. The directory is also user configurable by setting `external-functions-plugin.dir` and the default value is `externalFunctionsPlugin`. An example configuration in `config.properties` is presented as below:
```
external-functions-plugin.dir=externalFunctionsPlugin
```
