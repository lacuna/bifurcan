package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.ChunkSort;
import io.lacuna.bifurcan.durable.SwapBuffer;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.ToIntFunction;

/**
 * The underlying encode/decode logic for DurableMap.
 * <p>
 * Encoding:
 * - the number of entries [VLQ]
 * - the number of HashSkipTable tiers [uint8]
 * - the number of SkipTable tiers [uint8]
 * - a HashSkipTable of hashes onto entry offsets
 * - a SkipTable of indices onto entry offsets
 * - zero or more HashMapEntries blocks
 * <p>
 * If there are fewer than two HashMapEntries blocks, then both tables are omitted, and the associated values set to 0.
 */
public class HashMap {

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

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortEntries(IList<IEntry<K, V>> entries, ToIntFunction<K> keyHash) {
    return Iterators.map(
        ChunkSort.sortedEntries(
            Lists.from(entries.size(), i -> new MapIndex(keyHash.applyAsInt(entries.nth(i).key()), i)),
            MapIndex::encode,
            MapIndex::decode,
            Comparator.comparingInt((MapIndex e) -> e.hash)),
        i -> {
          IEntry<K, V> e = entries.nth(i.index);
          return IEntry.of(i.hash, e.key(), e.value());
        });
  }

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortEntryStream(Iterator<IEntry<K, V>> entries, int spillThreshold, DurableEncoding encoding) {
    long index = 0;
    ChunkSort.Accumulator<MapIndex> sorter = new ChunkSort.Accumulator<>(
        MapIndex::encode,
        MapIndex::decode,
        Comparator.comparingInt((MapIndex e) -> e.hash));

    IList<IEntry<K, V>> spilled = Lists.EMPTY;
    LinearList<IEntry<K, V>> curr = new LinearList<>();
    while (entries.hasNext()) {
      IEntry<K, V> e = entries.next();
      sorter.add(new MapIndex(encoding.keyHash().applyAsInt(e.key()), index++));

      curr.addLast(e);
      if (curr.size() == spillThreshold) {
        // TODO: actually encode `curr`
        spilled = spilled.concat(curr);
        curr = new LinearList<>();
      }
    }

    IList<IEntry<K, V>> es = spilled.concat(curr);
    return Iterators.map(sorter.sortedIterator(), i -> {
      IEntry<K, V> e = es.nth(i.index);
      return IEntry.of(i.hash, e.key(), e.value());
    });
  }

  /// encoding

  public static <K, V> void encodeUnsortedEntries(IList<IEntry<K, V>> entries, DurableEncoding encoding, DurableOutput out) {
    encodeSortedEntries(sortEntries(entries, (ToIntFunction<K>) encoding.keyHash()), encoding, out);
  }

  public static <K, V> void encodeSortedEntries(Iterator<IEntry.WithHash<K, V>> sortedEntries, DurableEncoding encoding, DurableOutput out) {
    // two tables and actual entries
    SwapBuffer entries = new SwapBuffer();
    SkipTable.Writer skipTable = new SkipTable.Writer();
    HashSkipTable.Writer hashTable = new HashSkipTable.Writer();

    // chunk up the entries so that collections are always singletons
    Iterator<Util.Block<IEntry.WithHash<K, V>, DurableEncoding>> entryBlocks = Util.partitionBy(
        sortedEntries,
        e -> encoding.valueEncoding(e.key()),
        DurableEncoding::blockSize,
        e -> Util.isCollection(e.key()) || Util.isCollection(e.value()));

    long index = 0;
    DurableEncoding keyEncoding = encoding.keyEncoding();
    while (entryBlocks.hasNext()) {
      Util.Block<IEntry.WithHash<K, V>, DurableEncoding> b = entryBlocks.next();

      // update the tables
      long offset = entries.written();
      skipTable.append(index, offset);
      hashTable.append(b.elements.stream().mapToInt(IEntry.WithHash::keyHash), offset);

      // write the entries
      HashMapEntries.encode(index, b, keyEncoding, entries);
      index += b.elements.size();
    }

    // flush everything to the provided sink
    long size = index;
    SwapBuffer.flushTo(out, BlockType.HASH_MAP, acc -> {
      acc.writeVLQ(size);

      // skip table metadata
      int tiers = skipTable.tiers();
      acc.writeUnsignedByte(tiers);

      // hash table metadata
      int bytesPerEntry = hashTable.tiers();
      acc.writeUnsignedByte(bytesPerEntry);

      if (tiers > 0) {
        skipTable.flushTo(acc);
      } else {
        skipTable.free();
      }

      if (bytesPerEntry > 0) {
        hashTable.flushTo(acc);
      } else {
        hashTable.free();
      }

      entries.flushTo(acc);
    });
  }

  /// decoding

  public static DurableMap decode(DurableInput in, IDurableCollection.Root root, DurableEncoding encoding) {
    DurableInput bytes = in.duplicate();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.HASH_MAP);
    long pos = in.position();

    long size = in.readVLQ();
    int skipTableTiers = in.readUnsignedByte();
    int hashTableTiers = in.readUnsignedByte();

    SkipTable skipTable = null;
    if (skipTableTiers > 0) {
      skipTable = new SkipTable(in.sliceBlock(BlockType.TABLE), skipTableTiers);
    }

    HashSkipTable hashTable = null;
    if (hashTableTiers > 0) {
      hashTable = new HashSkipTable(in.sliceBlock(BlockType.TABLE), hashTableTiers);
    }

    DurableInput entries = in.sliceBytes((pos + prefix.length) - in.position());

    return new DurableMap(bytes, root, size, hashTable, skipTable, entries, encoding);
  }

}
