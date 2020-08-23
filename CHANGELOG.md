# 0.2.0

### Fixes

* fixed issue where `DirectedGraph.edges()` returned edges pointing in the wrong direction
* fixed issue in `List` where series of operations including `slice()` could lead to a degenerate structure which made lookups impossible.  If you haven't seen mysterious `IndexOutOfBoundsException`s, this didn't affect you.
* fixed surprising implementation detail where `List` couldn't hold more than `Integer.MAX_VALUE` elements
* fixed issue where `list.toList().equals(...)` would always return true no matter the input
* fixed issue where `IntMap.slice(min, max)` would omit any negative entries if `min` was negative and `max` was non-negative
* fixed issue where `LinearSet.union` would update the collection in-place if given a set which was not also a `LinearSet`

### Changes

* moved `Lists.EMPTY`, `Sets.EMPTY`, and `Maps.EMPTY` to `List.EMPTY`, `Set.EMPTY`, and `Map.EMPTY` respectively
* moved `Lists.VirtualList`, `Sets.VirtualSet`, and `Maps.VirtualMap` to `diffs.DiffList`, `diffs.DiffSet`, and `diffs.DiffMap` respectively
* moved `Lists.Concat` to `diffs.ConcatList`
* subsumed `Lists.Slice` into `diffs.DiffList`
* made all hash functions (`IMap.keyHash`, `ISet.valueHash`, `IGraph.vertexHash`) yield `long`s instead of `int`s.
* changed `indexOf` methods to return `OptionalInt` instead of an `int` which is `-1` if no such element is found
