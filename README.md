![](doc/labyrinth.jpg)

```xml
<dependency>
  <groupId>io.lacuna</groupId>
  <artifactId>bifurcan</artifactId>
  <version>0.2.0-alpha1</version>
</dependency>
```

This library provides high-quality Java implementations of mutable and immutable data structures, each sharing a common API and these design principles:

* efficient random access
* efficient inverted indices (all collections other than lists provide an `indexOf` method)
* efficient splitting and merging of collections
* customizable equality semantics
* contiguous memory used wherever possible
* performance equivalent to, or better than, existing alternatives
* [ALPHA] durable (disk-backed) representations that share the API and broad performance characteristics of their in-memory counterparts

Rather than using the existing collection interfaces in `java.util` such as `List` or `Map`, it provides its own interfaces (`IList`, `IMap`, `ISet`) that provide functional semantics - each update to a collection returns a reference to a new collection.  Each interface provides a method (`toList`, `toMap`, `toSet`) for coercing the collection to a read-only version of the standard Java interfaces.

### what makes this better?

Some aspects of this library, like the inverted indices and durable collections, are unique.  

There are, however, many existing implementations of "functional" (aka persistent, immutable) data structures on the JVM.  As shown in [these in-depth comparisons](https://github.com/lacuna/bifurcan/blob/master/doc/comparison.md), Bifurcan's performance is equivalent to the best existing implementations for basic operations, and significantly better for batch operations such as `union`, `intersection`, and `difference`.

These optimized batch operations [require a high degree of complexity](https://github.com/lacuna/bifurcan/blob/29cb1efa4e561312ed7678a03c5ccedafc56e47d/src/io/lacuna/bifurcan/nodes/IntMapNodes.java#L665-L923) and are difficult to test, so it's understandable other library authors haven't bothered.  Bifurcan relies on [extensive generative tests](https://github.com/lacuna/bifurcan/blob/29cb1efa4e561312ed7678a03c5ccedafc56e47d/test/bifurcan/collection_test.clj) to validate its implementation, which makes this additional complexity easier to manage.

### collections

* [LinearMap](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/LinearMap.html) is a mutable hash-map, which allows for custom hash and equality semantics.  It stores entries contiguously in memory, which means that iteration over the entries can be [20x faster](https://github.com/lacuna/bifurcan/raw/master/benchmarks/images/map_iterate.png) than `java.util.HashMap` for larger collections.
* [Map](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/Map.html) is an immutable hash-map, which also allows for custom hash and equality semantics.  It ensures that all equivalent collections have an equivalent layout in memory, which makes checking for equality and performing set operations (`merge`, `union`, `difference`, `intersection`) significantly faster.
* [LinearSet](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/LinearSet.html) and [Set](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/Set.html) are built atop their respective map implementations, and have similar properties.
* [LinearList](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/LinearList.html) is a mutable list, which allows for elements to be added or removed from both ends of the collection, and allows random reads and writes within the list.
* [List](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/List.html) is an immutable list, which also allows for modification at both ends, as well as random reads and writes.  Due to its [relaxed radix structure](https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf), it also allows for near constant-time slices and concatenation.
* [SortedMap](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/SortedMap.html) is an immutable sorted map, built with a red-black tree.
* [IntMap](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/IntMap.html) is an immutable sorted map of integers onto arbitrary values, and can be used as an efficient sparse vector.  [FloatMap](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/FloatMap.html) provides similar functionality for floating-point keys.
* [Rope](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/Rope.html) is an immutable tree-based sequence of Unicode characters.  Unlike Java's `String`, it uses UTF-8 encoding and can efficiently index via both full code points and Java's preferred UTF-16 code units.
* [Graph](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/Graph.html), [DirectedGraph](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/DirectedGraph.html), and [DirectedAcyclicGraph](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/DirectedAcyclicGraph.html) implementations, which provide immutable graph data structures.

Full documentation can be [found here](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/package-summary.html).

### 'linear' and 'forked' collections

If we pass a mutable data structure into a function, we have to know what that function intends to do with it.  If it updates the data structure, we cannot safely read from it afterwards.  If it stores the data structure, we cannot safely write to it afterwards.  In other words, to use a mutable data structure safely we must ensure it has a single owner.  Enforcing this may require us to hold a large amount of code in our head at once.

Immutable data structures free us from having to care.  Functions can update or hold onto data without any risks.  We can reason locally about the flow of data, without any care for what the rest of the code is doing.  This can be enormously freeing.

This freedom, however, comes at a cost.  Updates to immutable data structures require a subset of the structure to be copied, which is much more expensive than simply overwriting existing memory.

If a data structure is referenced in multiple places, this is usually a cost worth paying.  In this case, however, it's just wasteful:

```java
Set<Long> set = new Set<>();
for (int i = 0; i < 1000; i++) {
  set = set.add(i);
}
```

This will create 999 intermediate copies of the set, none of which we care about.  There is only a single reference to these copies, and each is discarded as soon as `add()` is called.  The dataflow of these calls form a simple, linear chain.  To have more than one reference, the dataflow must diverge.

Where the dataflow is linear, we can safely use mutable collections.  Where it is not, we prefer to use immutable collections.  Since this linear flow is a local property, we would also like mutability to be a local property:

```java
Set<Long> set = new Set<>().linear();
for (int i = 0; i < 1000; i++) {
  set.add(i);
}
set = set.forked();
```

The call to `linear()` indicates that the collection has a single owner, and may be updated in-place.  The call to `forked()` indicates that this is no longer true.  By allowing temporary mutability, we gain huge performance benefits.  There is still a cost, however, relative to purely mutable data structures.  For this reason, Bifurcan provides permanently linear variants of each collection:

```java
LinearSet<Long> set = new LinearSet<>();
for (int i = 0; i < 1000; i++) {
  set.add(i);
}
```

If we call `forked()` on this collection, it will be wrapped in an immutable "diff" facade which tracks changes without touching the underlying collection.  These facades have similar performance to typical collections, but do not support efficient set operations.

### virtual collections

These facades also allow us to define collections programmatically:

```java
// a list of numbers within [0,1e6)
IList<Long> list = Lists.from(1_000_000, i -> i);
	
// the set of numbers within [0,1e6)
ISet<Long> set = Sets.from(list, i -> (0 <= i && i < list.size()) ? OptionalLong.of(i) : OptionalLong.empty());
	
// a map of numbers within [0,1e6) onto their square
IMap<Long, Long> map = Maps.from(set, i -> i * i);
```

These collections are not realized in-memory, and can be used as a translation layer for other data structure implementations.  Using our facades, however, we can still update them like any other collection, and only those changes will be directly represented in-memory.

### durable collections

[a short explanation, with a link to a longer explanation, goes here, in the meantime [here are some graphs](doc/durable.md)]

### no lazy collections

Most libraries for "functional programming" provide a lazy list or stream construct, but this is less a data structure than a mechanism for control flow.  While useful, such a mechanism is out of scope for this library.

### splitting and merging

Many modern data structure libraries also provide "parallel" collections, which make it easy to use multiple cores to process a single data structure.  These collections, however, are simply normal data structures with an execution model bolted on, without any obvious way to disentangle the two.

Rather than provide its own execution model, Bifurcan allows any collection to split into sub-collections using `split(k)`, which will return approximately `k` pieces, depending on its size.  The sub-collections can be processed and then merged using methods such as `concat`, `merge`, `union`, `intersection`, or `difference`.

This separation of concerns provides greater flexibility, but also requires more work to get up and running.

### license

Copyright © 2016-2018 Zachary Tellman

Distributed under the MIT License
