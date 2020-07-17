
Binary Functions and Operators
==============================

Binary Operators
----------------

The `||` operator performs concatenation.

Binary Functions
----------------

**length(binary)** -\> bigint

Returns the length of `binary` in bytes.

**concat(binary1, \..., binaryN)** -\> varbinary

Returns the concatenation of `binary1`, `binary2`, `...`, `binaryN`. This function provides the same functionality as the SQL-standard concatenation operator (`||`).

**substr(binary, start)** -\> varbinary

Returns the rest of `binary` from the starting position `start`, measured in bytes. Positions start with `1`. A negative starting position is interpreted as being relative to the end of the string.

**substr(binary, start, length)** -\> varbinary

Returns a substring from `binary` of length `length` from the starting position `start`, measured in bytes. Positions start with `1`. A negative starting position is interpreted as being relative to the end
of the string.

**to\_base64(binary)** -\> varchar

Encodes `binary` into a base64 string representation.

**from\_base64(string)** -\> varbinary

Decodes binary data from the base64 encoded `string`.

**to\_base64url(binary)** -\> varchar

Encodes `binary` into a base64 string representation using the URL safe alphabet.


**from\_base64url(string)** -\> varbinary

Decodes binary data from the base64 encoded `string` using the URL safe alphabet.

**to\_hex(binary)** -\> varchar

Encodes `binary` into a hex string representation.

**from\_hex(string)** -\> varbinary

Decodes binary data from the hex encoded `string`.

**to\_big\_endian\_64(bigint)** -\> varbinary

Encodes `bigint` in a 64-bit 2\'s complement big endian format.

**from\_big\_endian\_64(binary)** -\> bigint

Decodes `bigint` value from a 64-bit 2\'s complement big endian `binary`.

**to\_big\_endian\_32(integer)** -\> varbinary

Encodes `integer` in a 32-bit 2\'s complement big endian format.

**from\_big\_endian\_32(binary)** -\> integer

Decodes `integer` value from a 32-bit 2\'s complement big endian`binary`.


**to\_ieee754\_32(real)** -\> varbinary

Encodes `real` in a 32-bit big-endian binary according to IEEE 754 single-precision floating-point format.


**from\_ieee754\_32(binary)** -\> real

Decodes the 32-bit big-endian `binary` in IEEE 754 single-precision floating-point format.


**to\_ieee754\_64(double)** -\> varbinary

Encodes `double` in a 64-bit big-endian binary according to IEEE 754 double-precision floating-point format.


**from\_ieee754\_64(binary)** -\> double

Decodes the 64-bit big-endian `binary` in IEEE 754 double-precision floating-point format.

**lpad(binary, size, padbinary)** -\> varbinary

Left pads `binary` to `size` bytes with `padbinary`. If `size` is less than the length of `binary`, the result is truncated to `size` characters. `size` must not be negative and `padbinary` must be non-empty.


**rpad(binary, size, padbinary)** -\> varbinary

Right pads `binary` to `size` bytes with `padbinary`. If `size` is less than the length of `binary`, the result is truncated to `size` characters. `size` must not be negative and `padbinary` must be non-empty.

**crc32(binary)** -\> bigint

Computes the CRC-32 of `binary`. For general purpose hashing, use `xxhash64`, as it is much faster and produces a better quality hash.

**md5(binary)** -\> varbinary

Computes the md5 hash of `binary`.


**sha1(binary)** -\> varbinary

Computes the sha1 hash of `binary`.


**sha256(binary)** -\> varbinary

Computes the sha256 hash of `binary`.


**sha512(binary)** -\> varbinary

Computes the sha512 hash of `binary`.


**xxhash64(binary)** -\> varbinary

Computes the xxhash64 hash of `binary`.


**spooky\_hash\_v2\_32(binary)** -\> varbinary

Computes the 32-bit SpookyHashV2 hash of `binary`.


**spooky\_hash\_v2\_64(binary)** -\> varbinary

Computes the 64-bit SpookyHashV2 hash of `binary`.


**hmac\_md5(binary, key)** -\> varbinary

Computes HMAC with md5 of `binary` with the given `key`.


**hmac\_sha1(binary, key)** -\> varbinary

Computes HMAC with sha1 of `binary` with the given `key`.


**hmac\_sha256(binary, key)** -\> varbinary

Computes HMAC with sha256 of `binary` with the given `key`.


**hmac\_sha512(binary, key)** -\> varbinary

Computes HMAC with sha512 of `binary` with the given `key`.

