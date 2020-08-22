package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;

import static io.lacuna.bifurcan.diffs.Util.skipIndices;
import static io.lacuna.bifurcan.durable.codecs.Core.decodeCollection;


public class DiffHashMap {

  public interface IDurableMap<K, V> extends IDiffMap<K, V>, io.lacuna.bifurcan.IMap.Durable<K, V> {
  }

  public static <K, V> void encodeDiffHashMap(IDiffMap<K, V> m, IDurableCollection underlying, IDurableEncoding.Map encoding, DurableOutput out) {
    DurableBuffer.flushTo(out, BlockType.DIFF_HASH_MAP, acc -> {
      // removed indices
      SkipTable.Writer removed = new SkipTable.Writer();
      m.removedIndices().forEach(l -> removed.append(l, 0));

      acc.writeUnsignedByte(removed.tiers());
      removed.flushTo(acc);

      // added entries
      HashMap.encodeSortedEntries(m.added().hashSortedEntries(), encoding, acc);

      // underlying
      Reference.encode(underlying, acc);
    });
  }

  public static <K, V> void inlineDiffs(IList<IDiffMap<K, V>> diffStack, IDurableEncoding.Map encoding, DurableOutput out) {
    DurableBuffer.flushTo(out, BlockType.DIFF_HASH_MAP, acc -> {
      // removed indices
      SkipTable.Writer removed = new SkipTable.Writer();
      IDiffMap.mergedRemovedIndices((IList) diffStack).forEachRemaining((long l) -> removed.append(l, 0));
      acc.writeUnsignedByte(removed.tiers());
      removed.flushTo(acc);

      // added entries
      HashMap.encodeSortedEntries(IDiffMap.mergedAddedEntries(diffStack), encoding, acc);

      // underlying
      Reference.encode((IDurableCollection) diffStack.first().underlying(), acc);
    });
  }

  public static <K, V> void inline(IList<IDiffMap<K, V>> diffStack, IDurableEncoding.Map encoding, DurableOutput out) {
    Iterator<IEntry.WithHash<K, V>> underlyingEntries =
        skipIndices(
            diffStack.first().underlying().hashSortedEntries(),
            IDiffMap.mergedRemovedIndices((IList) diffStack));

    Iterator<IEntry.WithHash<K, V>> entries =
        Iterators.mergeSort(
            LinearList.of(underlyingEntries, IDiffMap.mergedAddedEntries(diffStack)),
            Comparator.comparing(IEntry.WithHash::keyHash));

    HashMap.encodeSortedEntries(entries, encoding, out);
  }

  public static <K, V> IMap.Durable<K, V> decodeDiffHashMap(IDurableEncoding.Map encoding, IDurableCollection.Root root, DurableInput.Pool bytes) {
    DurableInput in = bytes.instance();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.DIFF_HASH_MAP);

    int tiers = in.readUnsignedByte();
    ISortedMap<Long, Long> m = SkipTable.decode(in.sliceBlock(BlockType.TABLE).pool(), tiers);
    ISortedSet<Long> removed = m.keys();
    IMap<K, V> added = (IMap<K, V>) decodeCollection(encoding, root, in.slicePrefixedBlock().pool());
    IMap<K, V> underlying = (IMap<K, V>) Reference.decode(in.slicePrefixedBlock().pool()).decodeCollection(encoding, root);

    return new IDurableMap<K, V>() {
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

      @Override
      public IMap<K, V> clone() {
        return this;
      }

      @Override
      public int hashCode() {
        return (int) Maps.hash(this);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof IMap) {
          return Maps.equals(this, (IMap<K, V>) obj);
        } else {
          return false;
        }
      }

      @Override
      public String toString() {
        return Maps.toString(this);
      }
    };
  }
}
