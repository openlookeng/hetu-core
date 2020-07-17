
# RESET SESSION

## 摘要

``` sql
RESET SESSION name
RESET SESSION catalog.name
```

## 说明

将会话属性值重置为默认值。

## 示例

``` sql
RESET SESSION optimize_hash_generation;
RESET SESSION hive.optimized_reader_enabled;
```

## 另请参见

[SET SESSION](./set-session.md)、[SHOW SESSION](./show-session.md)