package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.HashMap;
import io.lacuna.bifurcan.durable.HashTable;
import io.lacuna.bifurcan.durable.SkipTable;

import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public class DurableHashMap implements IDurableCollection, IMap<Object, Object> {

  private final DurableInput in;
  private final long size, hashTablePosition, skipTablePosition, entriesPosition;
  private final DurableEncoding encoding;

  private DurableHashMap(
      DurableInput in,
      DurableEncoding encoding,
      long size,
      long hashTablePosition,
      long skipTablePosition,
      long entriesPosition) {
    this.in = in;
    this.encoding = encoding;

    this.size = size;

    this.hashTablePosition = hashTablePosition;
    this.skipTablePosition = skipTablePosition;
    this.entriesPosition = entriesPosition;
  }

  static DurableHashMap from(DurableInput in, DurableEncoding encoding) {
    long size = in.readVLQ();
    long hashTableSize = in.readVLQ();
    long skipTableSize = in.readVLQ();
    long hashTablePosition = in.position();

    return new DurableHashMap(
        in,
        encoding, size,
        hashTablePosition,
        hashTablePosition + hashTableSize,
        hashTablePosition + hashTableSize + skipTableSize);
  }

  @Override
  public ToIntFunction<Object> keyHash() {
    return encoding.keyHash();
  }

  @Override
  public BiPredicate<Object, Object> keyEquality() {
    return encoding.keyEquality();
  }

  @Override
  public Object get(Object key, Object defaultValue) {
    int hash = keyHash().applyAsInt(key);

    in.seek(hashTablePosition);
    HashTable.Entry blockEntry = HashTable.get(in, hash);
    if (blockEntry == null) {
      return defaultValue;
    } else {
      in.seek(blockEntry.offset);
      return HashMap.keyWithinBlock(in, encoding, hash, key, defaultValue);
    }
  }

  @Override
  public long indexOf(Object key) {
    return 0;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IEntry<Object, Object> nth(long index) {
    in.seek(skipTablePosition);
    SkipTable.Entry blockEntry = SkipTable.lookup(in, index);

    in.seek(blockEntry.offset);
    return HashMap.indexWithinBlock(in, encoding, (int) (index - blockEntry.index));
  }

  @Override
  public DurableHashMap clone() {
    return this;
  }
}
