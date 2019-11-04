package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiPredicate;
import java.util.function.IntConsumer;
import java.util.function.ToIntFunction;
import java.util.stream.LongStream;

import static io.lacuna.bifurcan.utils.IntIterators.deltas;

public class HashMap {

  private static double LOAD_FACTOR = 0.98;

  public static class MapEntry<K, V> {
    public final int hash;
    public final K key;
    public final V value;

    public MapEntry(int hash, IEntry<K, V> e) {
      this.hash = hash;
      this.key = e.key();
      this.value = e.value();
    }
  }

  /// sorting

  private static class MapIndex {
    final int hash;
    final long index;

    MapIndex(int hash, long index) {
      this.hash = hash;
      this.index = index;
    }

    static void encode(MapIndex e, DurableOutput out) {
      out.writeInt(e.hash);
      out.writeVLQ(e.index);
    }

    static MapIndex decode(DurableInput in) {
      int hash = in.readInt();
      long index = in.readVLQ();
      return new MapIndex(hash, index);
    }
  }

  public static <K, V> Iterator<MapEntry<K, V>> sortedMapEntries(IMap<K, V> m, ToIntFunction<K> keyHash) {
    return Iterators.map(
        ChunkSort.sortedEntries(
            () -> LongStream.range(0, m.size())
                .mapToObj(i -> new MapIndex(keyHash.applyAsInt(m.nth(i).key()), i))
                .iterator(),
            MapIndex::encode,
            MapIndex::decode,
            Comparator.comparingInt((MapIndex e) -> e.hash)),
        i -> new MapEntry<>(i.hash, m.nth(i.index)));
  }

  /// encoding

  private static class EntryEncoding {
    public final DurableEncoding key, value;

    public EntryEncoding(DurableEncoding key, DurableEncoding value) {
      this.key = key;
      this.value = value;
    }

    public int blockSize() {
      return Math.max(key.blockSize(), value.blockSize());
    }

    public boolean compatibleWith(EntryEncoding e) {
      return key.descriptor().equals(e.key.descriptor()) && value.descriptor().equals(e.value.descriptor());
    }
  }

  public static <K, V> void encode(IMap<K, V> m, DurableEncoding encoding, DurableOutput out) {
    // two tables and actual entries
    DurableAccumulator entries = new DurableAccumulator();
    SkipTable.Writer skipTable = new SkipTable.Writer();
    HashTable.Writer hashTable = HashTable.create(m.size(), LOAD_FACTOR);

    // sort entries according to their hash
    Iterator<MapEntry<K, V>> sortedEntries = sortedMapEntries(m, (ToIntFunction<K>) encoding.keyHash());

    // chunk up the entries so that collections are always singletons
    Iterator<Util.Block<MapEntry<K, V>, DurableEncoding>> entryBlocks = Util.partitionBy(
        sortedEntries,
        e -> encoding.valueEncoding(e.key),
        DurableEncoding::blockSize,
        (a, b) -> a.descriptor().equals(b.descriptor()),
        e -> Util.isCollection(e.key) || Util.isCollection(e.value));

    long index = 0;
    DurableEncoding keyEncoding = encoding.keyEncoding();
    while (entryBlocks.hasNext()) {
      Util.Block<MapEntry<K, V>, DurableEncoding> b = entryBlocks.next();
      MapEntry<K, V> first = b.elements.first();
      long offset = entries.written();

      // update the tables
      skipTable.append(index, offset);
      b.elements.forEach(e -> hashTable.put(e.hash, offset));

      // write the number of entries in this block
      entries.writeVLQ(b.elements.size());

      // write delta encoded hashes for each entry
      DurableAccumulator.flushTo(entries, BlockType.TABLE, false, acc -> {
        acc.writeInt(first.hash);
        deltas(b.elements.stream().mapToInt(e -> e.hash).iterator())
            .forEachRemaining((IntConsumer) acc::writeVLQ);
      });

      // write the key(s)
      if (b.isCollection && Util.isCollection(first.key)) {
        Util.encodeCollection(first.key, keyEncoding, entries);
      } else {
        DurableAccumulator.flushTo(entries, BlockType.ENCODED, false, acc ->
            keyEncoding.encode(Lists.lazyMap(b.elements, e -> e.key), acc));
      }

      // write the value(s)
      if (b.isCollection && Util.isCollection(first.value)) {
        Util.encodeCollection(first.value, b.encoding, entries);
      } else {
        DurableAccumulator.flushTo(entries, BlockType.ENCODED, false, acc ->
            b.encoding.encode(Lists.lazyMap(b.elements, e -> e.value), acc));
      }

      index += b.elements.size();
    }

    // flush everything to the provided sink
    DurableAccumulator.flushTo(out, BlockType.HASH_MAP, true, acc -> {
      acc.writeVLQ(m.size());
      acc.writeVLQ(hashTable.size());
      acc.writeVLQ(skipTable.size());
      hashTable.flushTo(acc);
      skipTable.flushTo(acc);
      entries.flushTo(acc);
    });
  }

  /// decoding

  private static class IndexRange {
    public final int start, end;

    public IndexRange(int start, int end) {
      this.start = start;
      this.end = end;
    }

    public boolean contains(int n) {
      return start <= n & n < end;
    }
  }

  private static IndexRange candidateIndices(DurableInput in, int hash, int numEntries) {
    int start = -1, end = numEntries;

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.TABLE);
    long endOfTable = in.position() + prefix.length;

    int currHash = in.readInt();

    int i = 0;
    if (currHash == hash) {
      start = end = 0;
    } else {
      for (i = 1; i < numEntries; i++) {
        currHash += in.readVLQ();
        if (currHash == hash) {
          start = i;
          break;
        }
      }
    }

    for (; i < numEntries; i++) {
      currHash += in.readVLQ();
      if (currHash > hash) {
        end = i;
        break;
      }
    }

    in.seek(endOfTable);
    return new IndexRange(start, end);
  }

  private static int matchingKeyIndex(DurableInput in, DurableEncoding mapEncoding, IndexRange candidates, Object key) {
    assert (candidates.start != -1);

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.ENCODED);
    long endOfKeys = in.position() + prefix.length;

    DurableEncoding.SkippableIterator it = mapEncoding.keyEncoding().decode(in).skip(candidates.start);

    BiPredicate<Object, Object> keyEquals = mapEncoding.keyEquality();
    int i = candidates.start;
    for (; i < candidates.end; i++) {
      Object k = it.next();
      if (keyEquals.test(k, key)) {
        break;
      }
    }

    in.seek(endOfKeys);
    return candidates.contains(i) ? i : -1;
  }

  public static Object keyWithinBlock(DurableInput in, DurableEncoding mapEncoding, int hash, Object key, Object defaultValue) {
    for (;;) {
      int numEntries = (int) in.readVLQ();

      IndexRange candidates = candidateIndices(in, hash, numEntries);
      if (candidates.start == -1) {
        return defaultValue;
      }

      int keyIndex = matchingKeyIndex(in, mapEncoding, candidates, key);
      if (keyIndex == -1 && candidates.end < numEntries) {
        return defaultValue;
      } else if (keyIndex != -1) {
        return mapEncoding.valueEncoding(key).decode(in).skip(keyIndex).next();
      }
    }
  }

  public static long indexOf(DurableInput in, DurableEncoding mapEncoding, Object key) {
    return -1;
  }

  public static IEntry<Object, Object> indexWithinBlock(DurableInput in, DurableEncoding mapEncoding, int index) {
    int numEntries = (int) in.readVLQ();
    assert (index >= 0 && index < numEntries);

    in.skipBlock();

    BlockPrefix keyPrefix = in.readPrefix();
    long endOfKeys = in.position() + keyPrefix.length;
    Object key = mapEncoding.keyEncoding().decode(in).skip(index).next();

    in.seek(endOfKeys);
    BlockPrefix valuePrefix = in.readPrefix();
    Object value = mapEncoding.valueEncoding(key).decode(in).skip(index).next();

    return new Maps.Entry<>(key, value);
  }

  public Iterator<MapEntry<Object, Object>> entries(long start) {
    return Iterators.EMPTY;
  }




}
