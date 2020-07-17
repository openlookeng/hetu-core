
# SET SESSION

## 摘要

``` sql
SET SESSION name = expression
SET SESSION catalog.name = expression
```

## 说明

设置会话属性值。

## 示例

``` sql
SET SESSION optimize_hash_generation = true;
SET SESSION hive.optimized_reader_enabled = true;
```

## 另请参见

[RESET SESSION](./reset-session.md)、[SHOW SESSION](./show-session.md)