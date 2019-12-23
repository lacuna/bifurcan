package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.blocks.List;
import io.lacuna.bifurcan.durable.blocks.SkipTable;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.file.Path;
import java.util.Iterator;

public class DurableList<V> implements IDurableCollection, IList<V> {

  private final DurableInput.Pool bytes;
  private final Root root;

  private final long size;
  private final IDurableEncoding.List encoding;
  private final SkipTable skipTable;
  private final DurableInput.Pool elements;

  public DurableList(DurableInput.Pool bytes, Root root, long size, SkipTable skipTable, DurableInput.Pool elements, IDurableEncoding.List encoding) {
    this.bytes = bytes;
    this.root = root;

    this.size = size;
    this.skipTable = skipTable;
    this.elements = elements;
    this.encoding = encoding;
  }

  public static <V> DurableList<V> open(Path path, IDurableEncoding.List encoding) {
    return (DurableList<V>) DurableCollections.open(path, encoding);
  }

  public static <V> void encode(Iterator<V> elements, IDurableEncoding.List encoding, DurableOutput out) {
    List.encode(elements, encoding, out);
  }

  public static <V> DurableList<V> decode(DurableInput.Pool pool, Root root, IDurableEncoding.List encoding) {
    return List.decode(pool, root, encoding);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public DurableList clone() {
    return this;
  }

  @Override
  public IDurableEncoding.List encoding() {
    return encoding;
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
  public V nth(long index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index + " must be within [0," + size() + ")");
    }
    SkipTable.Entry entry = skipTable == null ? SkipTable.Entry.ORIGIN : skipTable.floor(index);
    return (V) Util.decodeBlock(elements.instance().seek(entry.offset), root, encoding.elementEncoding())
        .skip(index - entry.index)
        .next();
  }

  @Override
  public Iterator<V> iterator() {
    DurableInput elements = this.elements.instance();
    return Iterators.flatMap(
        Iterators.from(elements::hasRemaining, elements::slicePrefixedBlock),
        in -> (Iterator<V>) Util.decodeBlock(in, root, encoding.elementEncoding()));
  }

  @Override
  public int hashCode() {
    return (int) Lists.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IList) {
      return Lists.equals(this, (IList) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Lists.toString(this);
  }
}
