
Mathematical Functions and Operators
====================================

Mathematical Operators
----------------------

| Operator | Description                                     |
| :------- | :---------------------------------------------- |
| `+`      | Addition                                        |
| `-`      | Subtraction                                     |
| `*`      | Multiplication                                  |
| `/`      | Division (integer division performs truncation) |
| `%`      | Modulus (remainder)                             |

Mathematical Functions
----------------------

**abs(x)** -\> \[same as input\]

Returns the absolute value of `x`.


**cbrt(x)** -\> double

Returns the cube root of `x`.


**ceil(x)** -\> \[same as input\]

This is an alias for `ceiling`.


**ceiling(x)** -\> \[same as input\]

Returns `x` rounded up to the nearest integer.


**cosine\_similarity(x, y)** -\> double

Returns the cosine similarity between the sparse vectors `x` and `y`:

    SELECT cosine_similarity(MAP(ARRAY['a'], ARRAY[1.0]), MAP(ARRAY['a'], ARRAY[2.0])); -- 1.0


**degrees(x)** -\> double

Converts angle `x` in radians to degrees.


**e()** -\> double

Returns the constant Euler\'s number.


**exp(x)** -\> double

Returns Euler\'s number raised to the power of `x`.


**floor(x)** -\> \[same as input\]

Returns `x` rounded down to the nearest integer.


**from\_base(string, radix)** -\> bigint

Returns the value of `string` interpreted as a base-`radix` number.


**inverse\_normal\_cdf(mean, sd, p)** -\> double

Compute the inverse of the Normal cdf with given mean and standard deviation (sd) for the cumulative probability (p): P(N \< n). The mean must be a real value and the standard deviation must be a real and
positive value. The probability p must lie on the interval (0, 1).

**normal\_cdf(mean, sd, v)** -\> double

Compute the Normal cdf with given mean and standard deviation (sd): P(N \< v; mean, sd). The mean and value v must be real values and the standard deviation must be a real and positive value.

**inverse\_beta\_cdf(a, b, p)** -\> double

Compute the inverse of the Beta cdf with given a, b parameters for the cumulative probability (p): P(N \< n). The a, b parameters must be positive real values. The probability p must lie on the interval \[0, 1\].

**beta\_cdf(a, b, v)** -\> double

Compute the Beta cdf with given a, b parameters: P(N \< v; a, b). The a, b parameters must be positive real numbers and value v must be a real value. The value v must lie on the interval \[0, 1\].


**ln(x)** -\> double

Returns the natural logarithm of `x`.


**log(b, x)** -\> double

Returns the base `b` logarithm of `x`.


**log2(x)** -\> double

Returns the base 2 logarithm of `x`.


**log10(x)** -\> double

Returns the base 10 logarithm of `x`.


**mod(n, m)** -\> \[same as input\]

Returns the modulus (remainder) of `n` divided by `m`.


**pi()** -\> double

Returns the constant Pi.


**pow(x, p)** -\> double

This is an alias for `power`.


**power(x, p)** -\> double

Returns `x` raised to the power of `p`.


**radians(x)** -\> double

Converts angle `x` in degrees to radians.


**rand()** -\> double

This is an alias for `random()`.


**random()** -\> double

Returns a pseudo-random value in the range 0.0 \<= x \< 1.0.


**random(n)** -\> \[same as input\]

Returns a pseudo-random number between 0 and n (exclusive).


**round(x)** -\> \[same as input\]

Returns `x` rounded to the nearest integer.


**round(x, d)** -\> \[same as input\]

Returns `x` rounded to `d` decimal places.


**sign(x)** -\> \[same as input\]

Returns the signum function of `x`, that is:

-   0 if the argument is 0,
-   1 if the argument is greater than 0,
-   -1 if the argument is less than 0.

For double arguments, the function additionally returns:

-   NaN if the argument is NaN,
-   1 if the argument is +Infinity,
-   -1 if the argument is -Infinity.


**sqrt(x)** -\> double

Returns the square root of `x`.


**to\_base(x, radix)** -\> varchar

Returns the base-`radix` representation of `x`.


**truncate(x)** -\> double

Returns `x` rounded to integer by dropping digits after decimal point.


**width\_bucket(x, bound1, bound2, n)** -\> bigint

Returns the bin number of `x` in an equi-width histogram with the specified `bound1` and `bound2` bounds and `n` number of buckets.

**width\_bucket(x, bins)** -\> bigint

Returns the bin number of `x` according to the bins specified by the array `bins`. The `bins` parameter must be an array of doubles and is assumed to be in sorted ascending order.


Statistical Functions
---------------------

**wilson\_interval\_lower(successes, trials, z)** -\> double

Returns the lower bound of the Wilson score interval of a Bernoulli trial process at a confidence specified by the z-score `z`.

**wilson\_interval\_upper(successes, trials, z)** -\> double

Returns the upper bound of the Wilson score interval of a Bernoulli trial process at a confidence specified by the z-score `z`.


Trigonometric Functions
-----------------------

All trigonometric function arguments are expressed in radians. See unit conversion functions `degrees` and
`radians`.

**acos(x)** -\> double

Returns the arc cosine of `x`.


**asin(x)** -\> double

Returns the arc sine of `x`.


**atan(x)** -\> double

Returns the arc tangent of `x`.


**atan2(y, x)** -\> double

Returns the arc tangent of `y / x`.


**cos(x)** -\> double

Returns the cosine of `x`.


**cosh(x)** -\> double

Returns the hyperbolic cosine of `x`.


**sin(x)** -\> double

Returns the sine of `x`.


**tan(x)** -\> double

Returns the tangent of `x`.


**tanh(x)** -\> double

Returns the hyperbolic tangent of `x`.


Floating Point Functions
------------------------

**infinity()** -\> double

Returns the constant representing positive infinity.


**is\_finite(x)** -\> boolean

Determine if `x` is finite.


**is\_infinite(x)** -\> boolean

Determine if `x` is infinite.


**is\_nan(x)** -\> boolean

Determine if `x` is not-a-number.


**nan()** -\> double

Returns the constant representing not-a-number.

