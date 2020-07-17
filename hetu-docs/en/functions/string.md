
String Functions and Operators
==============================

String Operators
----------------

The `||` operator performs concatenation.

String Functions
----------------

**Note**

These functions assume that the input strings contain valid UTF-8 encoded Unicode code points. There are no explicit checks for valid UTF-8 and the functions may return incorrect results on invalid UTF-8.
Invalid UTF-8 data can be corrected with `from_utf8`.

Additionally, the functions operate on Unicode code points and not user visible *characters* (or *grapheme clusters*). Some languages combine multiple code points into a single user-perceived *character*, the basic
unit of a writing system for a language, but the functions will treat each code point as a separate unit.

The `lower` and `upper` functions do not perform locale-sensitive, context-sensitive, or one-to-many mappings required for some languages. 

Specifically, this will return incorrect results for Lithuanian, Turkish and Azeri.

**chr(n)** -\> varchar

Returns the Unicode code point `n` as a single character string.

**codepoint(string)** -\> integer

Returns the Unicode code point of the only character of `string`.

**concat(string1, \..., stringN)** -\> varchar

Returns the concatenation of `string1`, `string2`, `...`, `stringN`. This function provides the same functionality as the SQL-standard concatenation operator (`||`).

**hamming\_distance(string1, string2)** -\> bigint

Returns the Hamming distance of `string1` and `string2`, i.e. the number  of positions at which the corresponding characters are different.

Note
that the two strings must have the same length.


**length(string)** -\> bigint

Returns the length of `string` in characters.


**levenshtein\_distance(string1, string2)** -\> bigint

Returns the Levenshtein edit distance of `string1` and `string2`, i.e. the minimum number of single-character edits (insertions, deletions or substitutions) needed to change `string1` into `string2`.

**lower(string)** -\> varchar

Converts `string` to lowercase.

**lpad(string, size, padstring)** -\> varchar

Left pads `string` to `size` characters with `padstring`. If `size` is less than the length of `string`, the result is truncated to `size` characters. `size` must not be negative and `padstring` must be non-empty.

**ltrim(string)** -\> varchar

Removes leading whitespace from `string`.

**replace(string, search)** -\> varchar

Removes all instances of `search` from `string`.

**replace(string, search, replace)** -\> varchar

Replaces all instances of `search` with `replace` in `string`.


**reverse(string)** -\> varchar

Returns `string` with the characters in reverse order.

**rpad(string, size, padstring)** -\> varchar

Right pads `string` to `size` characters with `padstring`. If `size` is less than the length of `string`, the result is truncated to `size` characters. `size` must not be negative and `padstring` must be non-empty.

**rtrim(string)** -\> varchar

Removes trailing whitespace from `string`.

**split(string, delimiter)** -\> array(varchar)

Splits `string` on `delimiter` and returns an array.

**split(string, delimiter, limit)** -\> array(varchar)

Splits `string` on `delimiter` and returns an array of size at most `limit`. The last element in the array always contain everything left in the `string`. `limit` must be a positive number.

**split\_part(string, delimiter, index)** -\> varchar

Splits `string` on `delimiter` and returns the field `index`. Field indexes start with `1`. If the index is larger than than the number of fields, then null is returned.

**split\_to**\_map(string, entryDelimiter, keyValueDelimiter) -\> map\<varchar, varchar\>

Splits `string` by `entryDelimiter` and `keyValueDelimiter` and returns a map. `entryDelimiter` splits `string` into key-value pairs. 

`keyValueDelimiter` splits each pair into key and value.

**split\_to**\_multimap(string, entryDelimiter, keyValueDelimiter) -\> map(varchar, array(varchar))

Splits `string` by `entryDelimiter` and `keyValueDelimiter` and returns a map containing an array of values for each unique key. 

`entryDelimiter` splits `string` into key-value pairs.  `keyValueDelimiter` splits each pair into key and value. The values for each key will be in the same order as they appeared in `string`.

**strpos(string, substring)** -\> bigint

Returns the starting position of the first instance of `substring` in `string`. Positions start with `1`. If not found, `0` is returned.

**position(substring IN string)** -\> bigint

Returns the starting position of the first instance of `substring` in `string`. Positions start with `1`. If not found, `0` is returned.

**substr(string, start)** -\> varchar

Returns the rest of `string` from the starting position `start`.
Positions start with `1`. A negative starting position is interpreted as being relative to the end of the string.

**substr(string, start, length)** -\> varchar

Returns a substring from `string` of length `length` from the starting position `start`. Positions start with `1`. A negative starting position is interpreted as being relative to the end of the string.

**trim(string)** -\> varchar

Removes leading and trailing whitespace from `string`.

**upper(string)** -\> varchar

Converts `string` to uppercase.

**word\_stem(word)** -\> varchar

Returns the stem of `word` in the English language.

**word\_stem(word, lang)** -\> varchar

Returns the stem of `word` in the `lang` language.


Unicode Functions
-----------------

**normalize(string)** -\> varchar

Transforms `string` with NFC normalization form.

**normalize(string, form)** -\> varchar

Transforms `string` with the specified normalization form. `form` must be be one of the following keywords:

| Form   | Description                                                  |
| :----- | :----------------------------------------------------------- |
| `NFD`  | Canonical Decomposition                                      |
| `NFC`  | Canonical Decomposition, followed by Canonical Composition   |
| `NFKD` | Compatibility Decomposition                                  |
| `NFKC` | Compatibility Decomposition, followed by Canonical Composition |

**Note**

This SQL-standard function has special syntax and requires specifying `form` as a keyword, not as a string.   

**to\_utf8(string)** -\> varbinary

Encodes `string` into a UTF-8 varbinary representation.

**from\_utf8(binary)** -\> varchar

Decodes a UTF-8 encoded string from `binary`. Invalid UTF-8 sequences are replaced with the Unicode replacement character `U+FFFD`.

**from\_utf8(binary, replace)** -\> varchar

Decodes a UTF-8 encoded string from `binary`. Invalid UTF-8 sequences are replaced with `replace`. The replacement string `replace` must either be a single character or empty (in which case invalid characters
are removed).

