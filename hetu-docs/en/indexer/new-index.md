# Adding your own Index Type

## Basic ideas

### ID

Each index type must have an `ID`, which is a unique identifier and the canonical name of an index type used in CLI, server config, file path, etc.

The `getID()` method in the `Index` interface returns the ID of this index type to any infrastructure that uses it.

### Level

A heuristic index stores additional and usually partial information of a dataset in a more compact way to speed up lookups in various ways. Therefore, each index must have a domain on which it is applied. For instance, if an index marks the max value of a data set, we must know how big the data set is when we define the "max" value (i.e.  it can be the max of a group of rows, a data partition, or even a whole table). When a new index type is created, it must implement a method `Set<Level> getSupportedIndexLevels();`  which returns the data set level it can support. The levels are defined as an enum in `Index` interface.

## Interface overlook

### Indexing methods

Apart from the methods mentioned above, this section gives a quick guide on the most important methods needed to create a new index type. For the complete document on `Index` interface, please refer to the Java Doc of the source code.

There are two main functionalities in the `Index` interface:

```java
boolean matches(Object expression) throws UnsupportedOperationException;

<I> Iterator<I> lookUp(Object expression) throws UnsupportedOperationException;
```

The first `matches()` method takes an expression, and returns if a predicate (the expression) can hold on a specific **Level** of data. For example, if an index records 
the max value of an integer column, it could easily tell if `col_val > 5` can hold by comparing `5` to the max value.

The second `lookUp()` method is optional. Instead of only returning a boolean about whether a predicate can hold, it returns an iterator of all the possible positions 
where the data can be found. An index should always have the same result on `matches()` and `lookUp().hasNext()`.

### Adding and persisting values

The following methods are used to add values to the index and persist index objects onto disk:

```java
boolean addValues(Map<String, List<Object>> values) throws IOException;

Index deserialize(InputStream in) throws IOException;

void serialize(OutputStream out) throws IOException;
```

The usage of them are pretty straightforward. A good example to help understand their usage is the source code of `MinMaxIndex`, where adding values is just to 
update the `max` and `min` variables according to the input number, and `serialize()/deserialize()`