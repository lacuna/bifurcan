package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Roots;
import io.lacuna.bifurcan.durable.codecs.List;
import io.lacuna.bifurcan.durable.io.FileOutput;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.file.Path;
import java.util.Iterator;

import static io.lacuna.bifurcan.durable.codecs.Core.decodeBlock;

public class DurableList<V> extends IList.Mixin<V> implements IList.Durable<V> {

  private final DurableInput.Pool bytes;
  private final Root root;

  private final long size;
  private final IDurableEncoding.List encoding;
  private final ISortedMap<Long, Long> indexTable;
  private final DurableInput.Pool elements;

  public DurableList(
      DurableInput.Pool bytes,
      Root root,
      long size,
      ISortedMap<Long, Long> indexTable,
      DurableInput.Pool elements,
      IDurableEncoding.List encoding
  ) {
    this.bytes = bytes;
    this.root = root;

    this.size = size;
    this.indexTable = indexTable;
    this.elements = elements;
    this.encoding = encoding;
  }

  public static <V> DurableList<V> open(IDurableEncoding.List encoding, Path path) {
    return Roots.open(path).decode(encoding);
  }

  public static <V> DurableList<V> from(Iterator<V> elements, IDurableEncoding.List encoding, Path directory) {
    Fingerprint f = FileOutput.write(directory, Map.empty(), acc -> List.encode(elements, encoding, acc));
    return Roots.open(directory, f).decode(encoding);
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
  public V nth(long idx) {
    if (idx < 0 || idx >= size) {
      throw new IndexOutOfBoundsException(idx + " must be within [0," + size() + ")");
    }
    IEntry<Long, Long> entry = indexTable.floor(idx);
    return (V) decodeBlock(elements.instance().seek(entry.value()), root, encoding.elementEncoding())
        .skip(idx - entry.key())
        .next();
  }

  @Override
  public Iterator<V> iterator() {
    // TODO: allow this to be consumed on different threads
    DurableInput elements = this.elements.instance();
    return Iterators.flatMap(
        Iterators.from(elements::hasRemaining, elements::slicePrefixedBlock),
        in -> (Iterator<V>) decodeBlock(in, root, encoding.elementEncoding())
    );
  }
}
