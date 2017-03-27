![](doc/labyrinth.jpg)

__This library is still alpha quality, use with caution__

This library provides high-quality Java implementations of mutable and immutable data structures, each sharing a common API and these design principles:

* efficient random access
* efficient splitting and merging of collections
* customizable equality semantics
* contiguous memory used wherever possible
* performance equivalent to, or better than, existing alternatives

Some of these properties, such as uniformly efficient random access and splits, are simply not available elsewhere.  Even if these are not required, other JVM libraries in this space tend to bring a large amount of tangential code along for the ride.  For instance, the collections in the [Functional Java](https://github.com/functionaljava/functionaljava) library assume and encourage the use of all the surrounding abstractions.  Clojure's data structures, while implemented in Java, are hard-coded to use Clojure's equality semantics.

These libraries are all-or-nothing propositions: they work great as long as you also adopt the surrounding ecosystem.  Historically, given the lack of functional primitives in Java's standard library, this made a lot of sense.  With the introduction of lambdas, streams, et al in Java 8, however, this is no longer true.

This library builds only on the primitives provided by the Java 8 standard library.  Rather than using the existing collection interfaces in `java.util` such as `List` or `Map`, it provides its own interfaces (`IList`, `IMap`, `ISet`) that provide functional semantics - each update to a collection returns a reference to a new collection.  Each interface provides a method (`toList`, `toMap`, `toSet`) for coercing the collection to a read-only version of the standard Java interfaces.

### collections

* [LinearMap](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/LinearMap.html) is a mutable hash-map, which allows for custom hash and equality semantics.  It stores entries contiguously in memory, which means that iteration over the entries can be [30-40x faster](https://github.com/lacuna/bifurcan/raw/master/benchmarks/images/map_iterate.png) than `java.util.HashMap` for larger collections.
* [Map](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/Map.html) is an immutable hash-map, which also allows for custom hash and equality semantics.  It ensures that all equivalent collections have an equivalent layout in memory, which makes checking for equality and performing set operations (`merge`, `union`, `difference`, `intersection`) significantly faster.  
* [LinearSet](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/LinearSet.html) and [Set](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/Set.html) are built atop their respective map implementations, and have similar properties.
* [LinearList](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/LinearList.html) is a mutable list, which allows for elements to be added or removed from both ends of the collection, and allows random reads and writes within the list.
* [List](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/List.html) is an immutable list, which also allows for modification at both ends, as well as random reads and writes.  Due to its [relaxed radix structure](https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf), it also allows for near constant-time slices and concatenation.
* [IntMap](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/IntMap.html) is an immutable sorted map of integers onto arbitrary values, and can be used as an efficient sparse vector.

Full benchmarks for these collections [can be found here](https://github.com/lacuna/bifurcan/blob/master/doc/benchmarks.md).  Full documentation [can be found here](http://lacuna.io/docs/bifurcan/io/lacuna/bifurcan/package-summary.html).

Bifurcan also allows for "virtual" collections, which can be defined using functions:

```java
// creates a list representing numbers from 0 to 999,999 without any backing memory
IList<Long> list = Lists.from(1_000_000, i -> i);

// creates a set representing that same range
ISet<Long> set = Sets.from(list, i -> 0 <= i && i < 1_000_000);
  
// creates a map of the numbers onto their corresponding square
IMap<Long, Long> map = Maps.from(set, i -> i * i);
```

Each of these collections are identical to their concrete, immutable equivalent.  They can be updated, in which case only the changes to the underlying collection will be stored in memory.

### "linear" and "forked" collections

If we pass a mutable data structure into a function, we have to know what that function intends to do with it.  If it updates the data structure, we cannot safely read from it afterwards.  If it stores the data structure, we cannot safely write to it afterwards.  In other words, to use a mutable data structure safely we must ensure it has a single owner.  Enforcing this may require us to hold a large amount of code in our head at once.

Immutable data structures free us from having to care.  Functions can update or hold onto data without any risks.  We can reason locally about the flow of data, without any care for what the rest of the code is doing.  This can be enormously freeing.

This freedom, however, comes at a cost.  Updates to immutable data structures require a subset of the structure to be copied, which is much more expensive than simply overwriting existing memory.

If a data structure is referenced in multiple places, this is a cost which is typically worth paying.  However, this is just wasteful:

```java
Set<Long> set = new Set();
for (int i = 0; i < 1000; i++) {
  set = set.add(i);
}
```

This will create 999 intermediate copies of the set, none of which we care about.  There is only a single reference to these copies, and each is discarded as soon as `add()` is called.  The dataflow of these calls form a simple, linear chain.  To have more than one reference, the dataflow must diverge.

Where the dataflow is linear, we can safely use mutable collections.  Where it is not, we prefer to use immutable collections.  Since this linear flow is a local property, we would also like mutability to be a local property:

```java
Set<Long> set = new Set().linear();
for (int i = 0; i < 1000; i++) {
  set.add(i);
}
set = set.forked();
```

The call to `linear()` indicates that the collection has a single owner, and may be updated in-place.  The call to `forked()` indicates that this is no true.  By allowing temporary mutability, we gain huge performance benefits.  However, there is still a cost relative to purely mutable data structures.  For this reason, Bifurcan provides permanently linear variants of each collection:

```java
LinearSet<Long> set = new LinearSet();
for (int i = 0; i < 1000; i++) {
  set.add(i);
}
```

If we call `forked()` on this collection, it simply will throw an exception.  All variants share the same API, however, allowing us to tweak the performance/safety tradeoffs of our code by changing only a few lines.

### no lazy collections

Most libraries for "functional programming" provide a lazy list or stream construct.  However, this is less a data structure than a mechanism for control flow.  While useful, such a mechanism is out of scope for this library.

### splitting and merging

Many modern data structure libraries also provide "parallel" collections, which make it easy to use multiple cores to process a single data structure.  However, these collections are simply normal data structures with an execution model bolted on, without any obvious way to disentangle the two.  

Rather than provide its own execution model, Bifurcan allows any collection to split into sub-collections using `split(k)`, which will return 1 to `k` pieces depending on its size.  The sub-collections can be processed and then merged using methods such as `concat`, `merge`, `union`, `intersection`, or `difference`.

This separation of concerns provides greater flexibility, but also requires more work to get up and running.

### license

Copyright © 2016-2017 Zachary Tellman

Distributed under the MIT License