package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Root;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.function.BiPredicate;

import static io.lacuna.bifurcan.durable.codecs.Core.decodeBlock;
import static io.lacuna.bifurcan.durable.codecs.Core.encodeBlock;

/**
 * A block that represents zero or more key/value pairs in a HashMap.
 * <p>
 * Encoding:
 * - the number of preceding entries [VLQ]
 * - the hash for each entry [{@link HashDeltas}]
 * - keys [an ENCODED block generated by {@link IDurableEncoding.Map#keyEncoding()}]
 * - values [an ENCODED block generated by {@link IDurableEncoding.Map#valueEncoding()}
 */
public class HashMapEntries {

  private static final BlockType BLOCK_TYPE = BlockType.TABLE;

  public static <K, V> void encode(
      long offset,
      IList<IEntry.WithHash<K, V>> block,
      IDurableEncoding.Map mapEncoding,
      DurableOutput out
  ) {
    DurableBuffer.flushTo(out, BLOCK_TYPE, acc -> {
      acc.writeUVLQ(offset);

      HashDeltas.Writer hashes = new HashDeltas.Writer();
      block.forEach(e -> hashes.append(e.keyHash()));
      hashes.flushTo(acc);

      encodeBlock(Lists.lazyMap(block, IEntry::key), mapEncoding.keyEncoding(), acc);
      encodeBlock(Lists.lazyMap(block, IEntry::value), mapEncoding.valueEncoding(), acc);
    });
  }

  public static HashMapEntries decode(DurableInput in, IDurableEncoding.Map mapEncoding, Root root) {
    DurableInput entries = in.sliceBlock(BLOCK_TYPE);
    long entryOffset = entries.readUVLQ();
    HashDeltas deltas = HashDeltas.decode(entries);
    DurableInput keys = entries.slicePrefixedBlock();
    DurableInput values = entries.slicePrefixedBlock();

    return new HashMapEntries(root, entryOffset, deltas, keys, values, mapEncoding);
  }

  public static Object get(Iterator<HashMapEntries> it, Root root, long hash, Object key, Object defaultValue) {
    while (it.hasNext()) {
      HashMapEntries entries = it.next();
      HashDeltas.IndexRange candidates = entries.hashes.candidateIndices(hash);

      int keyIndex = entries.localIndexOf(candidates, key);
      if (keyIndex == -1 && candidates.isBounded) {
        return defaultValue;
      } else if (keyIndex != -1) {
        return decodeBlock(entries.values, root, entries.mapEncoding.valueEncoding()).skip(keyIndex).next();
      }
    }

    return defaultValue;
  }

  public static OptionalLong indexOf(Iterator<HashMapEntries> it, long hash, Object key) {
    while (it.hasNext()) {
      HashMapEntries entries = it.next();
      HashDeltas.IndexRange candidates = entries.hashes.candidateIndices(hash);

      int keyIndex = entries.localIndexOf(candidates, key);
      if (keyIndex == -1 && candidates.isBounded) {
        return OptionalLong.empty();
      } else if (keyIndex != -1) {
        return OptionalLong.of(entries.indexOffset + keyIndex);
      }
    }

    return OptionalLong.empty();
  }

  public final long indexOffset;
  public final HashDeltas hashes;
  public final DurableInput keys, values;
  public final IDurableEncoding.Map mapEncoding;
  public final Root root;

  private HashMapEntries(
      Root root,
      long indexOffset,
      HashDeltas hashes,
      DurableInput keys,
      DurableInput values,
      IDurableEncoding.Map mapEncoding
  ) {
    this.root = root;
    this.indexOffset = indexOffset;
    this.hashes = hashes;
    this.keys = keys;
    this.values = values;
    this.mapEncoding = mapEncoding;
  }

  private int localIndexOf(HashDeltas.IndexRange candidates, Object key) {
    if (candidates.start < 0) {
      return -1;
    }

    IDurableEncoding.SkippableIterator it = decodeBlock(keys, root, mapEncoding.keyEncoding()).skip(candidates.start);
    BiPredicate<Object, Object> keyEquals = mapEncoding.keyEncoding().equalityFn();

    for (int i = candidates.start; i < candidates.end; i++) {
      Object k = it.next();
      if (keyEquals.test(k, key)) {
        return i;
      }
    }

    return -1;
  }

  public boolean isSingleton() {
    PrimitiveIterator.OfLong it = hashes.iterator();
    it.nextLong();
    return it.hasNext();
  }

  public IEntry.WithHash<Object, Object> nth(long index) {
    Object key = decodeBlock(keys, root, mapEncoding.keyEncoding()).skip(index).next();
    Object value = decodeBlock(values, root, mapEncoding.valueEncoding()).skip(index).next();

    return IEntry.of(hashes.nth(index), key, value);
  }

  public Iterator<IEntry.WithHash<Object, Object>> entries(long dropped) {
    PrimitiveIterator.OfLong hashes = (PrimitiveIterator.OfLong) Iterators.drop(this.hashes.iterator(), dropped);
    IDurableEncoding.SkippableIterator keys = decodeBlock(this.keys, root, mapEncoding.keyEncoding()).skip(dropped);
    IDurableEncoding.SkippableIterator values = decodeBlock(
        this.values,
        root,
        mapEncoding.valueEncoding()
    ).skip(dropped);
    return Iterators.from(hashes::hasNext, () -> IEntry.of(hashes.nextLong(), keys.next(), values.next()));
  }
}
