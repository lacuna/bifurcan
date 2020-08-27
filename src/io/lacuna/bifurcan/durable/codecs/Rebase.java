package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.Fingerprints;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.durable.io.FileOutput;

public class Rebase implements IDurableCollection.Rebase {

  private final Fingerprint original, updated;
  private final ISortedMap<Long, Long> updatedIndices;
  private final IDurableCollection.Root root;

  public Rebase(
      IDurableCollection.Root root,
      Fingerprint original,
      Fingerprint updated,
      ISortedMap<Long, Long> updatedIndices
  ) {
    this.original = original;
    this.updated = updated;
    this.updatedIndices = updatedIndices;
    this.root = root;
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
    return null;
  }

  public static Rebase encode(IDurableCollection.Root original, Fingerprint updated, SkipTable.Writer remappedIndices) {
    Fingerprint rebase = FileOutput.write(original.path().getParent(), Map.empty(), out -> {
      DurableBuffer.flushTo(out, BlockPrefix.BlockType.REBASE, acc -> {
        Fingerprints.encode(original.fingerprint(), out);
        Fingerprints.encode(updated, out);
        remappedIndices.flushTo(out);
      });
    });

    return decode(original.open(rebase));
  }

  public static Rebase decode(IDurableCollection.Root root) {
    DurableInput in = root.bytes().instance();
    Fingerprint original = Fingerprints.decode(in);
    Fingerprint updated = Fingerprints.decode(in);
    ISortedMap<Long, Long> updatedIndices = SkipTable.decode(root, in);
    return new Rebase(root, original, updated, updatedIndices);
  }


}
