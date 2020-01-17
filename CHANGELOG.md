# 0.2.0

### Fixes

* fixed issue where `DirectedGraph.edges()` returned edges pointing in the wrong direction
* fixed issue in `List` where series of operations including `slice()` could lead to a degenerate structure which made lookups impossible.  If you haven't seen mysterious `IndexOutOfBoundsException`s, this didn't affect you.

### Changes

* moved `Lists.EMPTY`, `Sets.EMPTY`, and `Maps.EMPTY` to `List.EMPTY`, `Set.EMPTY`, and `Map.EMPTY` respectively
* moved `Lists.VirtualList`, `Sets.VirtualSet`, and `Maps.VirtualMap` to `diffs.DiffList`, `diffs.DiffSet`, and `diffs.DiffMap` respectively
* moved `Lists.Concat` to `diffs.ConcatList`
* subsumed `Lists.Slice` into `diffs.DiffList`
* made all hash functions (`IMap.keyHash`, `ISet.valueHash`, `IGraph.vertexHash`) yield `long`s instead of `int`s.