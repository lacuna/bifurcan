package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Dependencies;
import io.lacuna.bifurcan.durable.FileOutput;
import io.lacuna.bifurcan.durable.DurableBuffer;
import io.lacuna.bifurcan.durable.blocks.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public class DurableMap<K, V> implements IDurableCollection, IMap<K, V> {
  private final IDurableEncoding.Map encoding;
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
      IDurableEncoding.Map encoding) {
    this.bytes = bytes;
    this.root = root;
    this.size = size;
    this.hashTable = hashTable;
    this.indexTable = indexTable;
    this.entries = entries;
    this.encoding = encoding;
  }

  public static <K, V> DurableMap<K, V> open(Path path, IDurableEncoding.Map encoding) {
    return (DurableMap<K, V>) DurableCollections.open(path, encoding);
  }

  public static <K, V> void encode(Iterator<IEntry<K, V>> entries, IDurableEncoding.Map encoding, int maxRealizedEntries, DurableOutput out) {
    HashMap.encodeSortedEntries(HashMap.sortEntries(entries, encoding, maxRealizedEntries), encoding, out);
  }

  public static <K, V> DurableMap<K, V> decode(DurableInput in, Root root, IDurableEncoding.Map encoding) {
    return HashMap.decode(in, root, encoding);
  }

  public static <K, V> DurableMap<K, V> from(Iterator<IEntry<K, V>> entries, IDurableEncoding.Map encoding, Path directory, int maxRealizedEntries) {
    Dependencies.enter();
    DurableBuffer acc = new DurableBuffer();
    encode(entries, encoding, maxRealizedEntries, acc);

    FileOutput file = new FileOutput(Dependencies.exit());
    DurableOutput out = DurableOutput.from(file);
    acc.flushTo(out);
    out.close();

    Path path = file.moveTo(directory);
    return (DurableMap<K, V>) DurableCollections.open(path, encoding);
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
  public IDurableEncoding.Map encoding() {
    return encoding;
  }

  @Override
  public ToIntFunction<K> keyHash() {
    return (ToIntFunction<K>) encoding.keyEncoding().hashFn();
  }

  @Override
  public BiPredicate<K, K> keyEquality() {
    return (BiPredicate<K, K>) encoding.keyEncoding().equalityFn();
  }

  @Override
  public V get(K key, V defaultValue) {
    int hash = keyHash().applyAsInt(key);
    HashSkipTable.Entry blockEntry = hashTable == null ? HashSkipTable.Entry.ORIGIN : hashTable.floor(hash);

    return blockEntry == null
        ? defaultValue
        : (V) HashMapEntries.get(chunkedEntries(blockEntry.offset), root, hash, key, defaultValue);
  }

  @Override
  public long indexOf(K key) {
    int hash = keyHash().applyAsInt(key);
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
  public IEntry.WithHash<K, V> nth(long index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index + " must be within [0," + size() + ")");
    }
    SkipTable.Entry blockEntry = indexTable == null ? SkipTable.Entry.ORIGIN : indexTable.floor(index);
    return (IEntry.WithHash<K, V>) chunkedEntries(blockEntry.offset).next().nth((int) (index - blockEntry.index));
  }

  @Override
  public DurableMap<K,V> clone() {
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
