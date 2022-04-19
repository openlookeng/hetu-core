#Extension Physical Execution Planner
This section describes how to add an extension physical execution planner in openLooKeng. With the extension physical execution planner, openLooKeng can utilize other operator acceleration libraries to speed up the execution of SQL statements.

##Configuration
To enable extension physical execution feature, the following configs must be added in
`config.properties`：

``` properties
extension_execution_planner_enabled=true
extension_execution_planner_jar_path=file:///xxPath/omni-openLooKeng-adapter-1.6.1-SNAPSHOT.jar
extension_execution_planner_class_path=nova.hetu.olk.OmniLocalExecutionPlanner
```

The above attributes are described below:

- `extension_execution_planner_enabled`: Enable extension physical execution feature.
- `extension_execution_planner_jar_path`: Set the file path of the extension physical execution jar package.
- `extension_execution_planner_class_path`: Set the package path of extension physical execution generated class in jar。


##Usage
The below command can control the enablement of extension physical execution feature in WebUI or Cli while running openLooKeng:
```
set session extension_execution_planner_enabled=true/false
```