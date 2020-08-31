package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Roots;
import io.lacuna.bifurcan.durable.codecs.HashMap;
import io.lacuna.bifurcan.durable.codecs.HashMapEntries;
import io.lacuna.bifurcan.durable.codecs.TempStream;
import io.lacuna.bifurcan.durable.io.FileOutput;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

public class DurableMap<K, V> extends IMap.Mixin<K, V> implements IMap.Durable<K, V> {
  private final IDurableEncoding.Map encoding;
  private final Root root;
  private final DurableInput.Pool bytes;

  private final long size;
  private final ISortedMap<Long, Long> hashTable;
  private final ISortedMap<Long, Long> indexTable;
  private final DurableInput.Pool entries;

  public DurableMap(
      DurableInput.Pool bytes,
      Root root,
      long size,
      ISortedMap<Long, Long> hashTable,
      ISortedMap<Long, Long> indexTable,
      DurableInput.Pool entries,
      IDurableEncoding.Map encoding
  ) {
    this.bytes = bytes;
    this.root = root;
    this.size = size;
    this.hashTable = hashTable;
    this.indexTable = indexTable;
    this.entries = entries;
    this.encoding = encoding;
  }

  public static <K, V> DurableMap<K, V> open(Path path, IDurableEncoding.Map encoding) {
    return Roots.open(path).decode(encoding);
  }

  public static <K, V> DurableMap<K, V> from(
      Iterator<IEntry<K, V>> entries,
      IDurableEncoding.Map encoding,
      Path directory,
      int maxRealizedEntries
  ) {
    Fingerprint f = FileOutput.write(
        directory,
        Map.empty(),
        acc -> {
          HashMap.encodeSortedEntries(HashMap.sortEntries(entries, encoding, maxRealizedEntries), encoding, acc);
          TempStream.pop();
        }
    );

    return Roots.open(directory, f).decode(encoding);
  }

  private Iterator<HashMapEntries> chunkedEntries(long offset) {
    // TODO: don't rely on this being realized on a single thread
    DurableInput in = entries.instance().seek(offset);
    return Iterators.from(
        () -> in.remaining() > 0,
        () -> HashMapEntries.decode(in, encoding, root)
    );
  }

  @Override
  public DurableInput.Pool bytes() {
    return bytes;
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
  public ToLongFunction<K> keyHash() {
    return (ToLongFunction<K>) encoding.keyEncoding().hashFn();
  }

  @Override
  public BiPredicate<K, K> keyEquality() {
    return (BiPredicate<K, K>) encoding.keyEncoding().equalityFn();
  }

  @Override
  public V get(K key, V defaultValue) {
    long hash = keyHash().applyAsLong(key);
    IEntry<Long, Long> blockEntry = hashTable.floor(hash);

    return blockEntry == null
        ? defaultValue
        : (V) HashMapEntries.get(chunkedEntries(blockEntry.value()), root, hash, key, defaultValue);
  }

  @Override
  public OptionalLong indexOf(K key) {
    long hash = keyHash().applyAsLong(key);
    IEntry<Long, Long> blockEntry = hashTable.floor(hash);

    return blockEntry == null
        ? OptionalLong.empty()
        : HashMapEntries.indexOf(chunkedEntries(blockEntry.value()), hash, key);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IEntry.WithHash<K, V> nth(long idx) {
    if (idx < 0 || idx >= size) {
      throw new IndexOutOfBoundsException(idx + " must be within [0," + size() + ")");
    }
    IEntry<Long, Long> blockEntry = indexTable.floor(idx);
    return (IEntry.WithHash<K, V>) chunkedEntries(blockEntry.value()).next().nth((int) (idx - blockEntry.key()));
  }

  @Override
  public Iterator<IEntry<K, V>> iterator() {
    return (Iterator) hashSortedEntries();
  }

  @Override
  public Iterator<IEntry.WithHash<K, V>> hashSortedEntries() {
    return Iterators.flatMap(
        chunkedEntries(0),
        chunk -> Iterators.map(
            chunk.entries(0),
            e -> IEntry.of(e.keyHash(), (K) e.key(), (V) e.value())
        )
    );
  }

  @Override
  public DurableMap<K, V> clone() {
    return this;
  }
}
