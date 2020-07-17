
START TRANSACTION
=================

Synopsis
--------

``` sql
START TRANSACTION [ mode [, ...] ]
```

where `mode` is one of

``` sql
ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
READ { ONLY | WRITE }
```

Description
-----------

Start a new transaction for the current session.

Examples
--------

``` sql
START TRANSACTION;
START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION READ WRITE;
START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;
START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE;
```

See Also
--------

[COMMIT](./commit.md), [ROLLBACK](./rollback.md)
