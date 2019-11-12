package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.ChunkSort;
import io.lacuna.bifurcan.durable.DurableAccumulator;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.ToIntFunction;

/**
 * The underlying encode/decode logic for DurableMap.
 *
 * Encoding:
 * - the number of entries [VLQ]
 * - the number of bytes per HashTable entry [uint8]
 * - the number of SkipTable tiers [uint8]
 * - a hash table of hashes onto entry offsets
 * - a skip table of indices onto entry offsets
 * - zero or more HashMapEntries blocks
 *
 * If there are fewer than two HashMapEntries blocks, then both tables are omitted, and the associated values set to 0.
 */
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

  public static <K, V> Iterator<MapEntry<K, V>> sortedMapEntries(IList<IEntry<K, V>> entries, ToIntFunction<K> keyHash) {
    return Iterators.map(
        ChunkSort.sortedEntries(
            Lists.from(entries.size(), i -> new MapIndex(keyHash.applyAsInt(entries.nth(i).key()), i)),
            MapIndex::encode,
            MapIndex::decode,
            Comparator.comparingInt((MapIndex e) -> e.hash)),
        i -> new MapEntry<>(i.hash, entries.nth(i.index)));
  }

  /// encoding

  public static ToIntFunction keyHash(DurableEncoding encoding) {
    return k -> {
      int hash = encoding.keyHash().applyAsInt(k);
      if (hash == HashTable.NONE) {
        hash = HashTable.FALLBACK;
      }
      return hash;
    };
  }

  // TODO: have this accept an iterator of sorted MapEntries
  public static <K, V> void encode(IList<IEntry<K, V>> mapEntries, DurableEncoding encoding, DurableOutput out) {
    // two tables and actual entries
    DurableAccumulator entries = new DurableAccumulator();
    SkipTable.Writer skipTable = new SkipTable.Writer();
    HashTable.Writer hashTable = new HashTable.Writer(LOAD_FACTOR);

    // sort entries according to their hash
    Iterator<MapEntry<K, V>> sortedEntries = sortedMapEntries(mapEntries, keyHash(encoding));

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

      // update the tables
      long offset = entries.written();
      skipTable.append(index, offset);
      b.elements.forEach(e -> hashTable.put(e.hash, offset));

      // write the entries
      HashMapEntries.encode(index, b, keyEncoding, entries);
      index += b.elements.size();
    }

    // flush everything to the provided sink
    DurableAccumulator.flushTo(out, BlockType.HASH_MAP, acc -> {
      acc.writeVLQ(mapEntries.size());

      // hash table metadata
      int bytesPerEntry = hashTable.entryBytes();
      acc.writeUnsignedByte(bytesPerEntry);

      // skip table metadata
      int tiers = skipTable.tiers();
      acc.writeUnsignedByte(tiers);

      if (bytesPerEntry > 0) {
        hashTable.flushTo(acc);
      }

      if (tiers > 0) {
        skipTable.flushTo(acc);
      }

      entries.flushTo(acc);
    });
  }

  /// decoding

  public static DurableMap decode(DurableInput in, DurableEncoding encoding) {
    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.HASH_MAP);
    long pos = in.position();

    long size = in.readVLQ();
    int bytesPerEntry = in.readUnsignedByte();
    int skipTableTiers = in.readUnsignedByte();

    HashTable hashTable = null;
    if (bytesPerEntry > 0) {
      hashTable = new HashTable(in.sliceBlock(BlockType.TABLE), bytesPerEntry);
    }

    SkipTable skipTable = null;
    if (skipTableTiers > 0) {
      skipTable = new SkipTable(in.sliceBlock(BlockType.TABLE), skipTableTiers);
    }

    DurableInput entries = in.sliceBytes((pos + prefix.length) - in.position());

    return new DurableMap(size, hashTable, skipTable, entries, encoding);
  }

}
