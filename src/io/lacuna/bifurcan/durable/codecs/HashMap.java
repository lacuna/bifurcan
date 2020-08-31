package io.lacuna.bifurcan.durable.codecs;

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
import java.util.function.ToLongFunction;

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
    final long hash;
    final long index;

    MapIndex(long hash, long index) {
      this.hash = hash;
      this.index = index;
    }

    static void encode(MapIndex e, DurableOutput out) {
      out.writeVLQ(e.hash);
      out.writeUVLQ(e.index);
    }

    static MapIndex decode(DurableInput in) {
      long hash = in.readVLQ();
      long index = in.readUVLQ();
      return new MapIndex(hash, index);
    }
  }

  private static final IDurableEncoding.Primitive MAP_INDEX_ENCODING =
      DurableEncodings.primitive("MapIndex", 32,
          DurableEncodings.Codec.from(
              (l, out) -> l.forEach(e -> MapIndex.encode((MapIndex) e, out)),
              (in, root) -> Iterators.skippable(Iterators.from(in::hasRemaining, () -> MapIndex.decode(in)))
          )
      );

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortIndexedEntries(
      ICollection<?, IEntry<K, V>> entries,
      ToLongFunction<K> keyHash
  ) {
    AtomicLong index = new AtomicLong(0);
    return Iterators.map(
        ChunkSort.sortedEntries(
            Iterators.map(entries.iterator(), e -> new MapIndex(keyHash.applyAsLong(e.key()), index.getAndIncrement())),
            Comparator.comparingLong((MapIndex e) -> e.hash),
            MAP_INDEX_ENCODING,
            1 << 16
        ),
        i -> {
          IEntry<K, V> e = entries.nth(i.index);
          return IEntry.of(i.hash, e.key(), e.value());
        }
    );
  }

  public static <K, V> Iterator<IEntry.WithHash<K, V>> sortEntries(
      Iterator<IEntry<K, V>> entries,
      IDurableEncoding.Map encoding,
      int maxRealizedEntries
  ) {
    IDurableEncoding hashEncoding = DurableEncodings.primitive("vlq", 1024,
        DurableEncodings.Codec.selfDelimited(
            (o, out) -> out.writeVLQ((long) o),
            (in, root) -> in.readVLQ()
        )
    );

    ToLongFunction<Object> hashFn = encoding.keyEncoding().hashFn();

    return ChunkSort.sortedEntries(
        Iterators.map(entries, e -> IEntry.of(hashFn.applyAsLong(e.key()), e.key(), e.value())),
        Comparator.comparingLong(IEntry.WithHash::keyHash),
        DurableEncodings.tuple(
            o -> {
              IEntry.WithHash<Object, Object> e = (IEntry.WithHash<Object, Object>) o;
              return new Object[]{e.keyHash(), e.key(), e.value()};
            },
            ary -> IEntry.of((long) ary[0], ary[1], ary[2]),
            hashEncoding,
            encoding.keyEncoding(),
            encoding.valueEncoding()
        ),
        maxRealizedEntries
    );
  }

  /// encoding

  public static <K, V> void encodeUnsortedEntries(
      IList<IEntry<K, V>> entries,
      IDurableEncoding.Map encoding,
      DurableOutput out
  ) {
    encodeSortedEntries(
        sortIndexedEntries(entries, (ToLongFunction<K>) encoding.keyEncoding().hashFn()),
        encoding,
        out
    );
    TempStream.pop();
  }

  public static <K, V> void encodeSortedEntries(
      Iterator<IEntry.WithHash<K, V>> sortedEntries,
      IDurableEncoding.Map encoding,
      DurableOutput out
  ) {
    // two tables and actual entries
    DurableBuffer entries = new DurableBuffer();
    SkipTable.Writer indexTable = new SkipTable.Writer();
    SkipTable.Writer hashTable = new SkipTable.Writer();

    // chunk up the entries so that collections are always singletons
    Iterator<IList<IEntry.WithHash<K, V>>> entryBlocks =
        Util.partitionBy(
            sortedEntries,
            Math.min(
                DurableEncodings.blockSize(encoding.keyEncoding()),
                DurableEncodings.blockSize(encoding.keyEncoding())
            ),
            e -> encoding.keyEncoding().isSingleton(e.key()) || encoding.valueEncoding().isSingleton(e.value())
        );

    long index = 0;
    long prevHash = Long.MAX_VALUE;
    while (entryBlocks.hasNext()) {
      IList<IEntry.WithHash<K, V>> b = entryBlocks.next();

      long offset = entries.written();

      // update nth() lookup
      indexTable.append(index, offset);

      // update get() lookup
      PrimitiveIterator.OfLong hashes = b.stream().mapToLong(IEntry.WithHash::keyHash).iterator();
      long firstHash = hashes.nextLong();
      if (firstHash != prevHash) {
        hashTable.append(firstHash, offset);
      }

      // if this block has a given hash, don't let the next block claim it
      while (hashes.hasNext()) {
        prevHash = hashes.nextLong();
      }

      // write the entries
      HashMapEntries.encode(index, b, encoding, entries);
      index += b.size();
    }

    flush(index, indexTable, hashTable, entries, out);
  }

  private static void flush(
      long size,
      SkipTable.Writer indexTable,
      SkipTable.Writer hashTable,
      DurableBuffer entries,
      DurableOutput out
  ) {
    DurableBuffer.flushTo(out, BlockType.HASH_MAP, acc -> {
      acc.writeUVLQ(size);
      indexTable.flushTo(acc);
      hashTable.flushTo(acc);
      entries.flushTo(acc);
    });
  }

  /// decoding

  private static final ISortedMap<Long, Long> DEFAULT_TABLE = new SortedMap<Long, Long>().put(Long.MIN_VALUE, 0L);

  public static DurableMap decode(IDurableEncoding.Map encoding, IDurableCollection.Root root, DurableInput.Pool pool) {
    DurableInput in = pool.instance();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.HASH_MAP);
    long pos = in.position();

    long size = in.readUVLQ();

    ISortedMap<Long, Long> skipTable = SkipTable.decode(root, in);
    if (skipTable.size() == 0) {
      skipTable = DEFAULT_TABLE;
    }

    ISortedMap<Long, Long> hashTable = SkipTable.decode(root, in);
    if (hashTable.size() == 0) {
      hashTable = DEFAULT_TABLE;
    }

    DurableInput.Pool entries = in.sliceBytes((pos + prefix.length) - in.position()).pool();

    return new DurableMap(pool, root, size, hashTable, skipTable, entries, encoding);


  }

  /// inlining

  /**
   * Effectively a fusion of {@link HashMap#encodeSortedEntries} and {@link HashMap#decode}.  Given a preexisting map,
   * write it to {@code out}, re-encoding each singleton entry such that they too can be inlined.
   */
  public static void inline(
      DurableInput.Pool pool,
      IDurableEncoding.Map encoding,
      IDurableCollection.Root root,
      DurableOutput out
  ) {
    DurableInput in = pool.instance();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockType.HASH_MAP);

    // skip over the existing tables
    long size = in.readUVLQ();
    int skipTableTiers = in.readUnsignedByte();
    int hashTableTiers = in.readUnsignedByte();
    if (skipTableTiers > 0) {
      in.skipBlock();
    }
    if (hashTableTiers > 0) {
      in.skipBlock();
    }

    // create buffers for the inlined map
    DurableBuffer entries = new DurableBuffer();
    SkipTable.Writer indexTable = new SkipTable.Writer();
    SkipTable.Writer hashTable = new SkipTable.Writer();

    long prevHash = Long.MAX_VALUE;
    Iterator<DurableInput> it = Iterators.from(in::hasRemaining, in::slicePrefixedBlock);
    while (it.hasNext()) {
      DurableInput block = it.next();
      HashMapEntries es = HashMapEntries.decode(block.duplicate(), encoding, root);
      long offset = entries.written();

      // update nth() lookup
      indexTable.append(es.indexOffset, offset);

      // update get() lookup
      PrimitiveIterator.OfLong hashes = es.hashes.iterator();
      long firstHash = hashes.nextLong();
      if (firstHash != prevHash) {
        hashTable.append(firstHash, offset);
      }

      // if this block has a given hash, don't let the next block claim it
      while (hashes.hasNext()) {
        prevHash = hashes.nextLong();
      }

      // if it's a singleton, re-encode it
      if (es.isSingleton()) {
        HashMapEntries.encode(es.indexOffset, LinearList.of(es.entries(0).next()), encoding, entries);

        // otherwise, just reuse the existing bytes
      } else {
        entries.transferFrom(block);
      }
    }

    flush(size, indexTable, hashTable, entries, out);
  }

}
