package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.ChunkSort;
import io.lacuna.bifurcan.durable.Fingerprints;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.durable.io.FileOutput;

import java.util.Comparator;

public class Rebase implements IDurableCollection.Rebase {

  private final Fingerprint original, updated;
  private final ISortedMap<Long, Long> updatedIndices;
  private final IDurableCollection.Root root;
  private final IDurableEncoding encoding;

  public Rebase(
      IDurableCollection.Root root,
      IDurableEncoding encoding,
      Fingerprint original,
      Fingerprint updated,
      ISortedMap<Long, Long> updatedIndices
  ) {
    this.original = original;
    this.updated = updated;
    this.updatedIndices = updatedIndices;
    this.root = root;
    this.encoding = encoding;
  }

  @Override
  public IDurableEncoding encoding() {
    return encoding;
  }

  @Override
  public Fingerprint original() {
    return original;
  }

  @Override
  public Fingerprint updated() {
    return updated;
  }

  @Override
  public ISortedMap<Long, Long> updatedIndices() {
    return updatedIndices;
  }

  @Override
  public IDurableCollection.Root root() {
    return root;
  }

  @Override
  public <T extends IDurableCollection> T apply(T collection) {
    return (T) root.open(Core.applyRebase(collection, this)).decode(encoding);
  }

  public static ChunkSort.Accumulator<IEntry<Long, Long>, ?> accumulator() {
    return ChunkSort.accumulator(
        Comparator.comparing(IEntry::key),
        DurableEncodings.tuple(e -> new Object[]{((IEntry) e).key(), ((IEntry) e).value()},
            ary -> IEntry.of(ary[0], ary[1]),
            DurableEncodings.INTEGERS,
            DurableEncodings.INTEGERS
        ),
        1 << 20
    );
  }

  public static Rebase encode(IDurableCollection original, Fingerprint updated, ChunkSort.Accumulator<IEntry<Long, Long>, ?> updatedIndices) {
    Fingerprint rebase = FileOutput.write(original.root(), Map.empty(), out -> {
      DurableBuffer.flushTo(out, BlockPrefix.BlockType.REBASE, acc -> {
        Fingerprints.encode(original.root().fingerprint(), out);
        Fingerprints.encode(updated, out);

        SkipTable.Writer w = new SkipTable.Writer();
        updatedIndices.sortedIterator().forEachRemaining(e -> w.append(e.key(), e.value()));
        TempStream.pop();
        w.flushTo(out);
      });
    });

    return decode(original.root().open(rebase), original.encoding());
  }

  public static Rebase decode(IDurableCollection.Root root, IDurableEncoding encoding) {
    DurableInput in = root.bytes().instance();
    Fingerprint original = Fingerprints.decode(in);
    Fingerprint updated = Fingerprints.decode(in);
    ISortedMap<Long, Long> updatedIndices = SkipTable.decode(root, in);
    return new Rebase(root, encoding, original, updated, updatedIndices);
  }


}
