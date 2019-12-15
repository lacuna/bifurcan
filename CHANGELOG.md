# 0.2.0

### Fixes

* fixed issue where `DirectedGraph.edges()` returned edges pointing in the wrong direction

### Changes

* moved `Lists.EMPTY`, `Sets.EMPTY`, and `Maps.EMPTY` to `List.EMPTY`, `Set.EMPTY`, and `Map.EMPTY` respectively
* moved `Lists.VirtualList`, `Sets.VirtualSet`, and `Maps.VirtualMap` to `diffs.DiffList`, `diffs.DiffSet`, and `diffs.DiffMap` respectively
* moved `Lists.Concat` to `diffs.ConcatList`
* subsumed `Lists.Slice` into `diffs.DiffList`

