package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.AccumulatorOutput;
import io.lacuna.bifurcan.durable.blocks.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public class DurableMap implements IDurableCollection, IMap<Object, Object> {
  private final DurableEncoding encoding;
  private final Root root;
  private final DurableInput bytes;

  private final long size;
  private final HashSkipTable hashTable;
  private final SkipTable indexTable;
  private final DurableInput entries;

  public DurableMap(
      DurableInput bytes,
      Root root,
      long size,
      HashSkipTable hashTable,
      SkipTable indexTable,
      DurableInput entries,
      DurableEncoding encoding) {
    this.bytes = bytes;
    this.root = root;
    this.size = size;
    this.hashTable = hashTable;
    this.indexTable = indexTable;
    this.entries = entries;
    this.encoding = encoding;
  }

  public static <K, V> DurableMap save(IMap<K, V> m, DurableEncoding encoding) {
    AccumulatorOutput out = new AccumulatorOutput(16 << 10, false);
    HashMap.encodeUnsortedEntries(m.entries(), encoding, out);
    return HashMap.decode(DurableInput.from(out.contents()), null, encoding);
  }

  private Iterator<HashMapEntries> chunkedEntries(long offset) {
    DurableInput in = entries.duplicate().seek(offset);
    return Iterators.from(
        () -> in.remaining() > 0,
        () -> HashMapEntries.decode(in, root, encoding));
  }

  @Override
  public DurableInput bytes() {
    return bytes.duplicate();
  }

  @Override
  public Root root() {
    return root;
  }

  @Override
  public DurableEncoding encoding() {
    return encoding;
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
    int hash = encoding.keyHash().applyAsInt(key);
    HashSkipTable.Entry blockEntry = hashTable == null ? HashSkipTable.Entry.ORIGIN : hashTable.floor(hash);

    return blockEntry == null
        ? defaultValue
        : HashMapEntries.get(chunkedEntries(blockEntry.offset), root, hash, key, defaultValue);
  }

  @Override
  public long indexOf(Object key) {
    int hash = encoding.keyHash().applyAsInt(key);
    HashSkipTable.Entry blockEntry = hashTable == null ? HashSkipTable.Entry.ORIGIN : hashTable.floor(hash);

    return blockEntry == null
        ? -1
        : HashMapEntries.indexOf(chunkedEntries(blockEntry.offset), hash, key);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IEntry.WithHash<Object, Object> nth(long index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index + " must be within [0," + size() + ")");
    }
    SkipTable.Entry blockEntry = indexTable == null ? SkipTable.Entry.ORIGIN : indexTable.floor(index);
    return chunkedEntries(blockEntry.offset).next().nth((int) (index - blockEntry.index));
  }

  @Override
  public DurableMap clone() {
    return this;
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return Maps.equals(this, (IMap) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }
}
