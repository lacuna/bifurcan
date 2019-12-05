package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.SwapBuffer;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.blocks.List;
import io.lacuna.bifurcan.durable.blocks.SkipTable;

import java.util.Iterator;

public class DurableList<V> implements IDurableCollection, IList<V> {

  private final DurableInput bytes;
  private final Root root;

  private final long size;
  private final DurableEncoding encoding;
  private final SkipTable skipTable;
  private final DurableInput elements;

  public DurableList(DurableInput bytes, Root root, long size, SkipTable skipTable, DurableInput elements, DurableEncoding encoding) {
    this.bytes = bytes;
    this.root = root;

    this.size = size;
    this.skipTable = skipTable;
    this.elements = elements;
    this.encoding = encoding;
  }

  public static <V> DurableList<V> save(Iterator<V> it, DurableEncoding encoding) {
    SwapBuffer out = new SwapBuffer(false);
    List.encode(it, encoding, out);
    return List.decode(DurableInput.from(out.contents()), null, encoding);
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
  public DurableEncoding encoding() {
    return encoding;
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
  public V nth(long index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index + " must be within [0," + size() + ")");
    }
    SkipTable.Entry entry = skipTable == null ? SkipTable.Entry.ORIGIN : skipTable.floor(index);
    return (V) Util.decodeBlock(elements.duplicate().seek(entry.offset), root, encoding.elementEncoding(entry.index))
        .skip(index - entry.index)
        .next();
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
