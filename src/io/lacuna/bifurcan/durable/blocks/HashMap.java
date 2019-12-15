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
import java.util.concurrent.atomic.AtomicLong;
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

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortIndexedEntries(ICollection<?, IEntry<K, V>> entries, ToIntFunction<K> keyHash) {
    AtomicLong index = new AtomicLong(0);
    return Iterators.map(
        ChunkSort.sortedEntries(
            Iterators.map(entries.iterator(), e -> new MapIndex(keyHash.applyAsInt(e.key()), index.getAndIncrement())),
            (it, out) -> it.forEach(e -> MapIndex.encode(e, out)),
            in -> Iterators.from(in::hasRemaining, () -> MapIndex.decode(in)),
            Comparator.comparingInt((MapIndex e) -> e.hash),
            1 << 16),
        i -> {
          IEntry<K, V> e = entries.nth(i.index);
          return IEntry.of(i.hash, e.key(), e.value());
        });
  }

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortEntries(Iterator<IEntry<K, V>> entries, IDurableEncoding.Map encoding, int maxRealizedEntries) {
    IDurableEncoding hashEncoding = DurableEncodings.primitive(
        "int32",
        1024,
        DurableEncodings.Codec.from(
            (l, out) -> l.forEach(n -> out.writeInt((int) n)),
            (in, root) -> Iterators.skippable(Iterators.from(in::hasRemaining, in::readInt))));

    ToIntFunction<Object> hashFn = encoding.keyEncoding().hashFn();

    return ChunkSort.sortedEntries(
        Iterators.map(entries, e -> IEntry.of(hashFn.applyAsInt(e.key()), e.key(), e.value())),
        Comparator.comparingInt(IEntry.WithHash::keyHash),
        DurableEncodings.list(
            DurableEncodings.tuple(
                o -> {
                  IEntry.WithHash<Object, Object> e = (IEntry.WithHash<Object, Object>) o;
                  return new Object[]{e.keyHash(), e.key(), e.value()};
                },
                ary -> IEntry.of((int) ary[0], ary[1], ary[2]),
                hashEncoding,
                encoding.keyEncoding(),
                encoding.valueEncoding())),
        maxRealizedEntries);
  }

  /// encoding

  public static <K, V> void encodeUnsortedEntries(IList<IEntry<K, V>> entries, IDurableEncoding.Map encoding, DurableOutput out) {
    encodeSortedEntries(sortIndexedEntries(entries, (ToIntFunction<K>) encoding.keyEncoding().hashFn()), encoding, out);
  }

  public static <K, V> void encodeSortedEntries(Iterator<IEntry.WithHash<K, V>> sortedEntries, IDurableEncoding.Map encoding, DurableOutput out) {
    // two tables and actual entries
    SwapBuffer entries = new SwapBuffer();
    SkipTable.Writer skipTable = new SkipTable.Writer();
    HashSkipTable.Writer hashTable = new HashSkipTable.Writer();

    // chunk up the entries so that collections are always singletons
    Iterator<IList<IEntry.WithHash<Object, Object>>> entryBlocks =
        Util.partitionBy(
            Iterators.map(sortedEntries, e -> IEntry.of(e.keyHash(), e.key(), e.value())),
            Math.min(encoding.keyEncoding().blockSize(), encoding.keyEncoding().blockSize()),
            e -> encoding.keyEncoding().isSingleton(e.key()) || encoding.valueEncoding().isSingleton(e.value()));

    long index = 0;
    while (entryBlocks.hasNext()) {
      IList<IEntry.WithHash<Object, Object>> b = entryBlocks.next();

      // update the tables
      long offset = entries.written();
      skipTable.append(index, offset);
      hashTable.append(b.stream().mapToInt(IEntry.WithHash::keyHash), offset);

      // write the entries
      HashMapEntries.encode(index, b, encoding, entries);
      index += b.size();
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

  public static DurableMap decode(DurableInput in, IDurableCollection.Root root, IDurableEncoding.Map encoding) {
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
