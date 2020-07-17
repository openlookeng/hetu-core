
# PREPARE

## 摘要

``` sql
PREPARE statement_name FROM statement
```

## 说明

准备一条语句，以便在以后执行。准备的语句是保存在具有给定名称的会话中的查询。该语句可以包含参数以代替字面量，这些参数在执行时被替换为实际的值。参数由问号表示。

## 示例

准备一个 select 查询：

    PREPARE my_select1 FROM
    SELECT * FROM nation;

准备一个包含参数的 select 查询。将与 `regionkey` 和 `nationkey` 进行比较的值将通过 `execute` 语句进行填充：

    PREPARE my_select2 FROM
    SELECT name FROM nation WHERE regionkey = ? AND nationkey < ?;

准备一个 insert 查询：

    PREPARE my_insert FROM
    INSERT INTO cities VALUES (1, 'San Francisco');

## 另请参见

[EXECUTE](./execute.md)、[DEALLOCATE PREPARE](./deallocate-prepare.md)、[DESCRIBE INPUT](./describe-input.md)、[DESCRIBE OUTPUT](./describe-output.md)