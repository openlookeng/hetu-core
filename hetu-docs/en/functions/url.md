
URL Functions
=============

Extraction Functions
--------------------

The URL extraction functions extract components from HTTP URLs (or any valid URIs conforming to [2396](https://tools.ietf.org/html/rfc2396.html). The following syntax is supported:

``` 
[protocol:][//host[:port]][path][?query][#fragment]
```

The extracted components do not contain URI syntax separators such as `:` or `?`.

**url\_extract\_fragment(url)** -\> varchar

Returns the fragment identifier from `url`.

**url\_extract\_host(url)** -\> varchar

Returns the host from `url`.

**url\_extract\_parameter(url, name** -\> varchar

Returns the value of the first query string parameter named `name` from `url`. Parameter extraction is handled in the typical manner as specified by [1866#section-8.2.1](https://tools.ietf.org/html/rfc1866.html#section-8.2.1).


**url\_extract\_path(url)** -\> varchar

Returns the path from `url`.

**url\_extract\_port(url)** -\> bigint

Returns the port number from `url`.


**url\_extract\_protocol(url)** -\> varchar

Returns the protocol from `url`.


**url\_extract\_query(url)** -\> varchar

Returns the query string from `url`.


Encoding Functions
------------------

**url\_encode(value)** -\> varchar

Escapes `value` by encoding it so that it can be safely included in URL query parameter names and values:

-   Alphanumeric characters are not encoded.
-   The characters `.`, `-`, `*` and `_` are not encoded.
-   The ASCII space character is encoded as `+`.
-   All other characters are converted to UTF-8 and the bytes are encoded as the string `%XX` where `XX` is the uppercase hexadecimal value of the UTF-8 byte.

**url\_decode(value)** -\> varchar

Unescapes the URL encoded `value`. This function is the inverse of `url_encode`.

