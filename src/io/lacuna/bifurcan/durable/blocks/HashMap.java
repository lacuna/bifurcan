package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.ChunkSort;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

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
      out.writeUVLQ(e.index);
    }

    static MapIndex decode(DurableInput in) {
      int hash = in.readInt();
      long index = in.readUVLQ();
      return new MapIndex(hash, index);
    }
  }

  private static final IDurableEncoding.Primitive MAP_INDEX_ENCODING =
    DurableEncodings.primitive("MapIndex", 32,
        DurableEncodings.Codec.from(
            (l, out) -> l.forEach(e -> MapIndex.encode((MapIndex) e, out)),
            (in, root) -> Iterators.skippable(Iterators.from(in::hasRemaining, () -> MapIndex.decode(in)))));

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortIndexedEntries(ICollection<?, IEntry<K, V>> entries, ToIntFunction<K> keyHash) {
    AtomicLong index = new AtomicLong(0);
    return Iterators.map(
        ChunkSort.sortedEntries(
            Iterators.map(entries.iterator(), e -> new MapIndex(keyHash.applyAsInt(e.key()), index.getAndIncrement())),
            Comparator.comparingInt((MapIndex e) -> e.hash),
            MAP_INDEX_ENCODING,
            1 << 16),
        i -> {
          IEntry<K, V> e = entries.nth(i.index);
          return IEntry.of(i.hash, e.key(), e.value());
        });
  }

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortEntries(Iterator<IEntry<K, V>> entries, IDurableEncoding.Map encoding, int maxRealizedEntries) {
    IDurableEncoding hashEncoding = DurableEncodings.primitive("int32", 1024,
        DurableEncodings.Codec.selfDelimited(
            (o, out) -> out.writeInt((int) o),
            (in, root) -> in.readInt()));

    ToIntFunction<Object> hashFn = encoding.keyEncoding().hashFn();

    return ChunkSort.sortedEntries(
        Iterators.map(entries, e -> IEntry.of(hashFn.applyAsInt(e.key()), e.key(), e.value())),
        Comparator.comparingInt(IEntry.WithHash::keyHash),
        DurableEncodings.tuple(
            o -> {
              IEntry.WithHash<Object, Object> e = (IEntry.WithHash<Object, Object>) o;
              return new Object[]{e.keyHash(), e.key(), e.value()};
            },
            ary -> IEntry.of((int) ary[0], ary[1], ary[2]),
            hashEncoding,
            encoding.keyEncoding(),
            encoding.valueEncoding()),
        maxRealizedEntries);
  }

  /// encoding

  public static <K, V> void encodeUnsortedEntries(IList<IEntry<K, V>> entries, IDurableEncoding.Map encoding, DurableOutput out) {
    encodeSortedEntries(sortIndexedEntries(entries, (ToIntFunction<K>) encoding.keyEncoding().hashFn()), encoding, out);
  }

  public static <K, V> void encodeSortedEntries(Iterator<IEntry.WithHash<K, V>> sortedEntries, IDurableEncoding.Map encoding, DurableOutput out) {
    // two tables and actual entries
    DurableBuffer entries = new DurableBuffer();
    SkipTable.Writer indexTable = new SkipTable.Writer();
    SkipTable.Writer hashTable = new SkipTable.Writer();

    // chunk up the entries so that collections are always singletons
    Iterator<IList<IEntry.WithHash<K, V>>> entryBlocks =
        Util.partitionBy(
            sortedEntries,
            Math.min(encoding.keyEncoding().blockSize(), encoding.keyEncoding().blockSize()),
            e -> encoding.keyEncoding().isSingleton(e.key()) || encoding.valueEncoding().isSingleton(e.value()));

    long index = 0;
    int prevHash = Integer.MAX_VALUE;
    while (entryBlocks.hasNext()) {
      IList<IEntry.WithHash<K, V>> b = entryBlocks.next();

      long offset = entries.written();

      // update nth() lookup
      indexTable.append(index, offset);

      // update get() lookup
      PrimitiveIterator.OfInt hashes = b.stream().mapToInt(IEntry.WithHash::keyHash).iterator();
      int firstHash = hashes.nextInt();
      if (firstHash != prevHash) {
        hashTable.append(firstHash, offset);
      }

      // if this block has a given hash, don't let the next block claim it
      while (hashes.hasNext()) {
        prevHash = hashes.nextInt();
      }

      // write the entries
      HashMapEntries.encode(index, b, encoding, entries);
      index += b.size();
    }

    TempStream.release();

    // flush everything to the provided sink
    long size = index;
    DurableBuffer.flushTo(out, BlockType.HASH_MAP, acc -> {
      acc.writeUVLQ(size);

      // skip table metadata
      int tiers = indexTable.tiers();
      acc.writeUnsignedByte(tiers);

      // hash table metadata
      int bytesPerEntry = hashTable.tiers();
      acc.writeUnsignedByte(bytesPerEntry);

      if (tiers > 0) {
        indexTable.flushTo(acc);
      } else {
        indexTable.free();
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

  public static DurableMap decode(IDurableEncoding.Map encoding, IDurableCollection.Root root, DurableInput.Pool pool) {
    DurableInput in = pool.instance();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.HASH_MAP);
    long pos = in.position();

    long size = in.readUVLQ();
    int skipTableTiers = in.readUnsignedByte();
    int hashTableTiers = in.readUnsignedByte();

    SkipTable skipTable = null;
    if (skipTableTiers > 0) {
      DurableInput skipIn = in.sliceBlock(BlockType.TABLE);
      skipTable = new SkipTable(root == null ? skipIn.pool() : () -> root.cached(skipIn), skipTableTiers);
    }

    SkipTable hashTable = null;
    if (hashTableTiers > 0) {
      DurableInput hashIn = in.sliceBlock(BlockType.TABLE);
      hashTable = new SkipTable(root == null ? hashIn.pool() : () -> root.cached(hashIn), hashTableTiers);
    }

    DurableInput.Pool entries = in.sliceBytes((pos + prefix.length) - in.position()).pool();

    return new DurableMap(pool, root, size, hashTable, skipTable, entries, encoding);
  }

}
