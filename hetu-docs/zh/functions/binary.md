
# 二进制函数和运算符

## 二进制运算符

`||` 运算符执行连接。

## 二进制函数

**length(binary)** -> bigint

返回 `binary` 的长度，以字节为单位。

**concat(binary1, ..., binaryN)** -> varbinary

返回 `binary1`、`binary2`、`...`、`binaryN` 的连接结果。该函数提供与 SQL 标准连接运算符 (`||`) 相同的功能。

**substr(binary, start)** -> varbinary

从起始位置 `start` 开始返回 `binary` 的其余部分，以字节为单位。位置从 `1` 开始。负起始位置表示相对于字符串的末尾。

**substr(binary, start, length)** -> varbinary

从起始位置 `start` 开始从 `binary` 返回长度为 `length` 的子字符串，以字节为单位。位置从 `1` 开始。负起始位置表示相对于字符串的末尾。

**to\_base64(binary)** -> varchar

将 `binary` 编码为 base64 字符串表示形式。

**from\_base64(string)** -> varbinary

从以 base64 编码的 `string` 解码二进制数据。

**to\_base64url(binary)** -> varchar

使用 URL 安全字母表将 `binary` 编码为 base64 字符串表示形式。

**from\_base64url(string)** -> varbinary

使用 URL 安全字母表从以 base64 编码的 `string` 解码二进制数据。

**to\_hex(binary)** -> varchar

将 `binary` 编码为十六进制字符串表示形式。

**from\_hex(string)** -> varbinary

从以十六进制编码的 `string` 解码二进制数据。

**to\_big\_endian\_64(bigint)** -> varbinary

以 64 位二进制补码大端字节序格式对 `bigint` 进行编码。

**from\_big\_endian\_64(binary)** -> bigint

从 64 位二进制补码大端字节序 `binary` 解码 `bigint` 值。

**to\_big\_endian\_32(integer)** -> varbinary

以 32 位二进制补码大端字节序格式对 `integer` 进行编码。

**from\_big\_endian\_32(binary)** -> integer

从 32 位二进制补码大端字节序 `binary` 解码 `integer` 值。

**to\_ieee754\_32(real)** -> varbinary

根据 IEEE 754 单精度浮点格式将 `real` 编码为 32 位大端字节序二进制数。

**from\_ieee754\_32(binary)** -> real

对采用 IEEE 754 单精度浮点格式的 32 位大端字节序 `binary` 进行解码。

**to\_ieee754\_64(double)** -> varbinary

根据 IEEE 754 双精度浮点格式将 `double` 编码为 64 位大端字节序二进制数。

**from\_ieee754\_64(binary)** -> double

对采用 IEEE 754 双精度浮点格式的 64 位大端字节序 `binary` 进行解码。

**lpad(binary, size, padbinary)** -> varbinary

使用 `padbinary` 将 `binary` 左填充至 `size` 个字节。如果 `size` 小于 `binary` 的长度，结果将被截断至 `size` 个字符。`size` 不得为负数，并且 `padbinary` 必须为非空值。

**rpad(binary, size, padbinary)** -> varbinary

使用 `padbinary` 将 `binary` 右填充至 `size` 个字节。如果 `size` 小于 `binary` 的长度，结果将被截断至 `size` 个字符。`size` 不得为负数，并且 `padbinary` 必须为非空值。

**crc32(binary)** -> bigint

计算 `binary` 的 CRC-32 值。对于通用哈希，使用 `xxhash64`，因为它快得多并且能生成质量更好的哈希值。

**md5(binary)** -> varbinary

计算 `binary` 的 md5 哈希值。

**sha1(binary)** -> varbinary

计算 `binary` 的 sha1 哈希值。

**sha256(binary)** -> varbinary

计算 `binary` 的 sha256 哈希值。

**sha512(binary)** -> varbinary

计算 `binary` 的 sha512 哈希值。

**xxhash64(binary)** -> varbinary

计算 `binary` 的 xxhash64 哈希值。

**spooky\_hash\_v2\_32(binary)** -> varbinary

计算 `binary` 的 32 位 SpookyHashV2 哈希值。

**spooky\_hash\_v2\_64(binary)** -> varbinary

计算 `binary` 的 64 位 SpookyHashV2 哈希值。

**hmac\_md5(binary, key)** -> varbinary

使用给定的 `key` 计算 `binary` 的 HMAC 值（采用 md5）。

**hmac\_sha1(binary, key)** -> varbinary

使用给定的 `key` 计算 `binary` 的 HMAC 值（采用 sha1）。

**hmac\_sha256(binary, key)** -> varbinary

使用给定的 `key` 计算 `binary` 的 HMAC 值（采用 sha256）。

**hmac\_sha512(binary, key)** -> varbinary

使用给定的 `key` 计算 `binary` 的 HMAC 值（采用 sha512）。