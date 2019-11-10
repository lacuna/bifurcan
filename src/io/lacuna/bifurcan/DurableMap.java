package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.DurableAccumulator;
import io.lacuna.bifurcan.durable.blocks.HashMap;
import io.lacuna.bifurcan.durable.blocks.HashMapEntries;
import io.lacuna.bifurcan.durable.blocks.HashTable;
import io.lacuna.bifurcan.durable.blocks.SkipTable;

import java.util.Iterator;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public class DurableMap implements IDurableCollection, IMap<Object, Object> {

  private final long size;
  private final HashTable.Reader hashTable;
  private final SkipTable.Reader skipTable;
  private final DurableInput entries;
  private final DurableEncoding encoding;
  private final ToIntFunction<Object> keyHash;

  public DurableMap(
      long size,
      HashTable.Reader hashTable,
      SkipTable.Reader skipTable,
      DurableInput entries,
      DurableEncoding encoding) {
    this.size = size;
    this.hashTable = hashTable;
    this.skipTable = skipTable;
    this.entries = entries;
    this.encoding = encoding;
    this.keyHash = HashMap.keyHash(encoding);
  }

  public static <K, V> DurableMap save(IMap<K, V> m, DurableEncoding encoding) {
    DurableAccumulator out = new DurableAccumulator();
    HashMap.encode(m.entries(), encoding, out);
    return HashMap.decode(DurableInput.from(out.contents()), encoding);
  }

  private Iterator<HashMapEntries.Reader> entries(long offset) {
    DurableInput in = entries.duplicate().seek(offset);
    return new Iterator<HashMapEntries.Reader>() {
      @Override
      public boolean hasNext() {
        return in.remaining() > 0;
      }

      @Override
      public HashMapEntries.Reader next() {
        return HashMapEntries.decode(in, encoding);
      }
    };
  }

  @Override
  public ToIntFunction<Object> keyHash() {
    return keyHash;
  }

  @Override
  public BiPredicate<Object, Object> keyEquality() {
    return encoding.keyEquality();
  }

  @Override
  public Object get(Object key, Object defaultValue) {
    int hash = keyHash.applyAsInt(key);

    HashTable.Entry blockEntry = hashTable == null ? HashTable.Entry.ENTRY : hashTable.get(hash);
    if (blockEntry == null) {
      return defaultValue;
    } else {
      Iterator<HashMapEntries.Reader> it = entries(blockEntry.offset);
      return HashMapEntries.get(it, hash, key, defaultValue);
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
    SkipTable.Entry blockEntry = skipTable == null ? SkipTable.Entry.ENTRY : skipTable.floor(index);
    return entries(blockEntry.offset).next().nth((int) (index - blockEntry.index));
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
