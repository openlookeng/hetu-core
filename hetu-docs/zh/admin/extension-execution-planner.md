# 扩展物理执行计划
本节介绍openLooKeng如何添加扩展物理执行计划。通过物理执行计划的扩展，openLooKeng可以使用其他算子加速库来加速SQL语句的执行。

## 配置
首先，需要从以下链接获取相关jar包和c++库：
- `boostkit-omniop-openlookeng-1.6.1-1.0.0-aarch64.jar`: [Link](https://github.com/kunpengcompute/boostkit-bigdata/releases/download/v1.3.0/boostkit-omniop-openlookeng-1.6.1-1.0.0-aarch64.zip)

- `libboostkit-omniop-xx-1.0.0-aarch.so`: [Link](https://www.hikunpeng.com/zh/developer/boostkit/big-data?acclerated=3)

然后，在配置文件`config.properties`增加如下配置：

``` properties
extension_execution_planner_enabled=true
extension_execution_planner_jar_path=file:///xxPath/boostkit-omniop-openlookeng-1.6.1-1.0.0-aarch64.jar
extension_execution_planner_class_path=nova.hetu.olk.OmniLocalExecutionPlanner
```

上述属性说明如下：

- `extension_execution_planner_enabled`：是否开启扩展物理执行计划特性。
- `extension_execution_planner_jar_path`：指定扩展jar包的文件路径。
- `extension_execution_planner_class_path`：指定扩展jar包中执行计划生成类的包路径。


## 使用
当运行openLooKeng时，可在WebUI或Cli中通过如下命令控制扩展物理执行计划的开启:
```
set session extension_execution_planner_enabled=true/false
```