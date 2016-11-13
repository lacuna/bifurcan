![](doc/labyrinth.jpg)

__This library is a work in progress, and should not be used yet__

This library provides flexible, high-quality implementations of collections in Java, all of which share these properties:

* "functional" API for both mutable and immutable collections, where each modification returns an updated collection
* efficient random access
* efficient splitting and merging of collections
* maps and sets allow for custom hash and equality semantics
* contiguous memory used wherever possible
* read/write performance equivalent to, or better than, existing alternatives

Some of these properties, such as uniformly efficient random access and split/merges, are simply not available elsewhere.  Even if these are not required, other JVM libraries in this space tend to bring a large amount of tangential code along for the ride.  For instance, the collections in the [Functional Java](https://github.com/functionaljava/functionaljava) library assume and encourage the use of all the surrounding abstractions.  Clojure's data structures, while implemented in Java, are hard-coded to use Clojure's equality semantics.

These libraries are all-or-nothing propositions: they work great as long as you also adopt the surrounding ecosystem.  Historically, given the lack of functional primitives in Java's standard library, this made a lot of sense.  With the introduction of lambdas, streams, et al in Java 8, however, this is no longer required.

This library builds only on the primitives provided by the Java 8 standard library.  Rather than using the existing collection interfaces in `java.util` such as `List` or `Map`, it provides its own interfaces (`IList`, `IMap`, `ISet`) that provide functional semantics - each update to a collection returns a reference to a new collection.  Each interface provides a method (`toList`, `toMap`, `toSet`) for coercing the collection to a read-only version of the standard Java interfaces.

### *linear* and *forked* collections

Functional collections are typically *persistent*, meaning that every version of the data structure can be accessed, even after changes are made.  This can be done efficiently by using trees for storage, and sharing common structure between all the versions.  Even so, these implementations require more time and memory than their mutable counterparts.

Persistent data structures keep us from having to worry what other functions or threads are doing to our data.  However, if no one else can see our data, we don't need such strong guarantees.  If we need to construct a map with a thousand entries, we don't care about every intermediate version, just the final one.

In the literature, non-persistent data structures are called *ephemeral*, and in Clojure they're called *transient*.  Any persistent Clojure data structure can be turned into a transient version of itself, updated, and then returned to a persistent state.  Since transient data structures are allowed to overwrite previous versions of themselves, they can narrow (but not close) the performance gap with mutable data structures.

Bifurcan provides both ephemeral and persistent implementations of each collection, called *linear* and *forked* respectively.  This is meant to reflect the data flow of each use case: an ephemeral collection has a single downstream consumer, while a persistent data structure may have many.

Any data structure can be turned into a linear or forked variant of itself via `linear()` and `forked()`.  However, in the special case where a forked variant isn't required, Bifurcan provides special `LinearList`, `LinearMap`, and `LinearSet` implementations that provide equivalent performance to Java's mutable collections.

### splitting and merging

Any collection can be split into sub-collections using `split(k)`, which will return 1 to `k` pieces, depending on the size of the collection.  These sub-collections can be merged using methods such as `concat`, `merge`, `union`, `intersection`, or `difference`, depending on the type of the collection.

This allows for parallel processing of the collections, but is not coupled to a particular execution model.  This makes the data structures more flexible than Java's parallel streams, or Scala's parallel collections, but does not provide any parallel execution out of the box.

### license

Copyright © 2016 Zachary Tellman

Distributed under the MIT License