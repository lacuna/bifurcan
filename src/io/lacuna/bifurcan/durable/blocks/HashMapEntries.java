package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.DurableAccumulator;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.function.BiPredicate;

public class HashMapEntries {

  private static final BlockType BLOCK_TYPE = BlockType.OTHER;

  public static <K, V> void encode(
      Util.Block<HashMap.MapEntry<K, V>, DurableEncoding> block,
      DurableEncoding keyEncoding,
      DurableOutput out) {
    DurableAccumulator.flushTo(out, BLOCK_TYPE, acc -> {
      acc.writeVLQ(block.elements.size());

      HashDeltas.Writer hashes = new HashDeltas.Writer();
      block.elements.forEach(e -> hashes.append(e.hash));
      hashes.flushTo(acc);

      Util.encodeBlock(Lists.lazyMap(block.elements, e -> e.key), keyEncoding, acc);
      Util.encodeBlock(Lists.lazyMap(block.elements, e -> e.value), block.encoding, acc);
    });
  }

  public static Reader decode(DurableInput in, DurableEncoding mapEncoding) {
    DurableInput entries = in.sliceBlock(BLOCK_TYPE);
    long numEntries = entries.readVLQ();
    HashDeltas.Reader deltas = HashDeltas.decode(entries);
    DurableInput keys = entries.slicePrefixedBlock();
    DurableInput values = entries.slicePrefixedBlock();

    return new Reader(numEntries, deltas, keys, values, mapEncoding);
  }

  public static Object get(Iterator<Reader> readers, int hash, Object key, Object defaultValue) {
    while (readers.hasNext()) {
      Reader entries = readers.next();
      HashDeltas.IndexRange candidates = entries.hashes.candidateIndices(hash);
      if (candidates.start == -1) {
        return defaultValue;
      }

      int keyIndex = entries.localIndexOf(candidates, key);
      if (keyIndex == -1 && candidates.isBounded) {
        return defaultValue;
      } else if (keyIndex != -1) {
        return Util.decodeBlock(entries.values, entries.mapEncoding.valueEncoding(key)).skip(keyIndex).next();
      }
    }
    return defaultValue;
  }

  public static class Reader {

    public final long numEntries;
    public final HashDeltas.Reader hashes;
    public final DurableInput keys, values;
    public final DurableEncoding mapEncoding;

    private Reader(
        long numEntries,
        HashDeltas.Reader hashes,
        DurableInput keys,
        DurableInput values,
        DurableEncoding mapEncoding) {
      this.numEntries = numEntries;
      this.hashes = hashes;
      this.keys = keys;
      this.values = values;
      this.mapEncoding = mapEncoding;
    }

    private int localIndexOf(HashDeltas.IndexRange candidates, Object key) {
      assert (candidates.start != -1);

      DurableEncoding.SkippableIterator it = Util.decodeBlock(keys, mapEncoding.keyEncoding()).skip(candidates.start);
      BiPredicate<Object, Object> keyEquals = mapEncoding.keyEquality();

      for (int i = candidates.start; i < candidates.end; i++) {
        Object k = it.next();
        if (keyEquals.test(k, key)) {
          return i;
        }
      }

      return -1;
    }

    public static long indexOf(int hash, Object key) {
      return -1;
    }

    public IEntry<Object, Object> nth(int index) {
      Object key = Util.decodeBlock(keys, mapEncoding.keyEncoding()).skip(index).next();
      Object value = Util.decodeBlock(values, mapEncoding.valueEncoding(key)).skip(index).next();

      return new Maps.Entry<>(key, value);
    }

    public Iterator<HashMap.MapEntry<Object, Object>> entries(long start) {
      return Iterators.EMPTY;
    }
  }
}
