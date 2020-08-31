package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.IDurableCollection.Root;
import io.lacuna.bifurcan.diffs.Util;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.ChunkSort;
import io.lacuna.bifurcan.durable.Dependencies;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.durable.io.FileOutput;
import io.lacuna.bifurcan.utils.Iterators;
import io.lacuna.bifurcan.utils.Iterators.Indexed;

import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;

import static io.lacuna.bifurcan.diffs.Util.skipIndices;
import static io.lacuna.bifurcan.durable.codecs.Core.decodeCollection;


public class DiffHashMap {

  public static PrimitiveIterator.OfLong mergedRemovedIndices(IList<IDiffMap<?, ?>> diffStack) {
    assert diffStack.stream().allMatch(m -> m instanceof IMap.Durable);

    // isolate the removed indices which only apply to the underlying collection (which is potentially shrinking with
    // each new stacked diff)
    IList<Iterator<Long>> iterators = new LinearList<>();
    long underlyingSize = diffStack.first().underlying().size();
    long removed = 0;
    for (IDiffMap<?, ?> m : diffStack) {
      long remainingUnderlyingSize = underlyingSize - removed;
      ISortedSet<Long> s = m.removedIndices().slice(0L, remainingUnderlyingSize - 1);
      iterators.addLast(s.iterator());
      removed += s.size();
    }

    return Util.mergedRemovedIndices(iterators);
  }

  public static <K, V> Iterator<Indexed<IEntry.WithHash<K, V>>> mergedAddedEntries(IList<IDiffMap<K, V>> diffStack) {
    assert diffStack.stream().allMatch(m -> m instanceof IMap.Durable);

    // isolate the removed indices which only apply to the added entries
    IList<Iterator<Long>> iterators = new LinearList<>();
    long underlyingSize = diffStack.first().underlying().size();
    for (IDiffMap<K, V> m : diffStack) {
      long currUnderlyingSize = underlyingSize;
      ISortedSet<Long> underlyingIndices = m.removedIndices().slice(0L, currUnderlyingSize - 1);
      ISortedSet<Long> addedIndices = m.removedIndices().slice(currUnderlyingSize, Long.MAX_VALUE);
      iterators.addLast(Iterators.map(addedIndices.iterator(), n -> n - currUnderlyingSize));
      underlyingSize -= underlyingIndices.size();
    }

    SkipTable.Writer writer = new SkipTable.Writer();
    Util.mergedRemovedIndices(iterators).forEachRemaining((long idx) -> writer.append(idx, 0));

    // for this to consume too much memory would require >100M entries being repeatedly overwritten within the stack
    // of diffs, which implies that many entries being in-memory at once, which seems far-fetched enough that I'm not
    // going to worry about it for now
    // TODO: worry about it
    ISortedSet<Long> removedIndices = writer.toOffHeapMap().keys();

    // get the hash-sorted entries (which are in the same order as entries() because it's a durable map) from each
    // added() and filter out the removed entries from each
    IList<Iterator<Indexed<IEntry.WithHash<K, V>>>> sortedEntries = new LinearList<>();
    long offset = 0;
    for (IDiffMap<K, V> m : diffStack) {
      long size = m.added().size();
      long currOffset = offset;
      sortedEntries.addLast(
          Util.skipIndices(
              Iterators.indexed(m.added().hashSortedEntries(), underlyingSize + offset),
              Iterators.map(removedIndices.slice(currOffset, currOffset + size - 1).iterator(), n -> n - currOffset)
          ));
      offset += size;
    }

    return Iterators.mergeSort(sortedEntries, Comparator.comparingLong(e -> e.value.keyHash()));
  }

  public static boolean hasUnderlying(IDurableCollection c, Fingerprint underlying, Fingerprint dependent) {
    Root r = c.root().open(dependent);
    if (r.bytes().instance().readPrefix().type == BlockType.DIFF_HASH_MAP) {
      AMap<Object, Object> m = r.decode(c.encoding());
      return ((IDurableCollection) m.underlying()).root().fingerprint().equals(underlying);
    } else {
      return false;
    }
  }

  public static <K, V> void encodeDiffHashMap(
      IDiffMap<K, V> m,
      IDurableCollection underlying,
      IDurableEncoding.Map encoding,
      DurableOutput out
  ) {
    DurableBuffer.flushTo(out, BlockType.DIFF_HASH_MAP, acc -> {
      // removed indices
      SkipTable.Writer removed = new SkipTable.Writer();
      m.removedIndices().forEach(l -> removed.append(l, 0));
      removed.flushTo(acc);

      // added entries
      HashMap.encodeSortedEntries(m.added().hashSortedEntries(), encoding, acc);

      // underlying
      Reference.encode(underlying, acc);
    });
  }

  public static <K, V> void inlineDiffs(
      IList<IDiffMap<K, V>> diffStack,
      IDurableEncoding.Map encoding,
      ChunkSort.Accumulator<IEntry<Long, Long>, ?> updatedIndices,
      DurableOutput out
  ) {
    DurableBuffer.flushTo(out, BlockType.DIFF_HASH_MAP, acc -> {
      // removed indices
      SkipTable.Writer removed = new SkipTable.Writer();
      mergedRemovedIndices((IList) diffStack).forEachRemaining((long l) -> removed.append(l, 0));
      removed.flushTo(acc);

      // added entries
      Iterator<Indexed<IEntry.WithHash<K, V>>> added = mergedAddedEntries(diffStack);

      // populate index table
      if (updatedIndices != null) {
        added = Iterators.map(Iterators.indexed(added), e -> {
          if (e.index != e.value.index) {
            updatedIndices.add(IEntry.of(e.value.index, e.index));
          }
          return e.value;
        });
      }

      HashMap.encodeSortedEntries(Iterators.map(added, e -> e.value), encoding, acc);

      // underlying
      Reference.encode((IDurableCollection) diffStack.first().underlying(), acc);
    });
  }

  public static <K, V> void inline(
      IList<IDiffMap<K, V>> diffStack,
      IDurableEncoding.Map encoding,
      ChunkSort.Accumulator<IEntry<Long, Long>, ?> updatedIndices,
      DurableOutput out
  ) {

    // underlying
    Iterator<IEntry.WithHash<K, V>> underlyingEntries =
        skipIndices(
            diffStack.first().underlying().hashSortedEntries(),
            mergedRemovedIndices((IList) diffStack)
        );

    // underlying ++ added
    Iterator<Indexed<IEntry.WithHash<K, V>>> entries =
        Iterators.mergeSort(
            LinearList.of(Iterators.indexed(underlyingEntries), mergedAddedEntries(diffStack)),
            Comparator.comparing(e -> e.value.keyHash())
        );

    // populate index table
    if (updatedIndices != null) {
      entries = Iterators.map(Iterators.indexed(entries), e -> {
        if (e.index != e.value.index) {
          updatedIndices.add(IEntry.of(e.value.index, e.index));
        }
        return e.value;
      });
    }

    HashMap.encodeSortedEntries(Iterators.map(entries, e -> e.value), encoding, out);
  }

  public static <K, V> IDurableCollection.Rebase rebase(IDiffMap<K, V> m, ISortedMap<Long, Long> updatedIndices) {
    IDurableCollection c = (IDurableCollection) m;

    Fingerprint updated = FileOutput.write(c.root(), Map.empty(), out -> {
      DurableBuffer.flushTo(out, BlockType.DIFF_HASH_MAP, acc -> {
        // removed indices
        SkipTable.Writer removed = new SkipTable.Writer();
        ChunkSort.sortedEntries(
            m.removedIndices().stream().map(i -> updatedIndices.get(i, i)).iterator(),
            Comparator.naturalOrder(),
            DurableEncodings.INTEGERS,
            (int) 1e6
        ).forEachRemaining(i -> removed.append(i, 0));
        TempStream.pop();
        removed.flushTo(acc);

        // added entries
        acc.transferFrom(((IDurableCollection) m.added()).bytes().instance());
        Reference.from((IDurableCollection) m.underlying()).encode(acc);

        // add in the dependencies manually, since we've just shifted over the bytes
        c.root().dependencies().forEach(Dependencies::add);
      });
    });

    ISortedSet<Long>
        originalRemoved = m.removedIndices(),
        rebasedRemoved = c.root().open(updated).<AMap<K, V>>decode(c.encoding()).removedIndices();
    ChunkSort.Accumulator<IEntry<Long, Long>, ?> rebasedUpdated = Rebase.accumulator();
    updatedIndices.entries().forEach(e -> {
      OptionalLong floorIdx = originalRemoved.floorIndex(e.key());
      if (!floorIdx.isPresent() || originalRemoved.nth(floorIdx.getAsLong()).longValue() != e.key()) {
        rebasedUpdated.add(IEntry.of(
            e.key() - floorIdx.orElse(0),
            e.value() - rebasedRemoved.floorIndex(e.value()).orElse(0)
        ));
      }
    });

    return Rebase.encode(c, updated, rebasedUpdated);
  }

  private static abstract class AMap<K, V> extends IMap.Mixin<K, V> implements IDiffMap<K, V>, IMap.Durable<K, V> {
  }

  public static <K, V> IMap.Durable<K, V> decodeDiffHashMap(
      IDurableEncoding.Map encoding,
      Root root,
      DurableInput.Pool bytes
  ) {
    DurableInput in = bytes.instance();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.DIFF_HASH_MAP);

    ISortedMap<Long, Long> m = SkipTable.decode(root, in);
    ISortedSet<Long> removed = m.keys();
    IMap<K, V> added = decodeCollection(encoding, root, in.slicePrefixedBlock().pool());
    IMap<K, V> underlying = Reference.decode(in.slicePrefixedBlock().pool()).decodeCollection(encoding, root);

    return new AMap<K, V>() {
      @Override
      public IMap<K, V> underlying() {
        return underlying;
      }

      @Override
      public IMap<K, V> added() {
        return added;
      }

      @Override
      public ISortedSet<Long> removedIndices() {
        return removed;
      }

      @Override
      public IDurableEncoding.Map encoding() {
        return encoding;
      }

      @Override
      public DurableInput.Pool bytes() {
        return bytes;
      }

      @Override
      public Root root() {
        return root;
      }
    };
  }
}
