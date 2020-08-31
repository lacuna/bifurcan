package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.IDurableCollection;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.IDurableEncoding;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.Dependencies;
import io.lacuna.bifurcan.durable.Fingerprints;
import io.lacuna.bifurcan.durable.io.DurableBuffer;

public class Reference {

  public final Fingerprint fingerprint;
  public final long position;

  private Reference(Fingerprint fingerprint, long position) {
    this.fingerprint = fingerprint;
    this.position = position;
  }

  public static Reference from(IDurableCollection c) {
    // get the offset from the start of `c` to the start of the root collection
    long pos = c.bytes().instance().bounds().absolute().start - c.root().bytes().instance().bounds().absolute().start;
    return new Reference(c.root().fingerprint(), pos);
  }

  public void encode(DurableOutput out) {
    DurableBuffer.flushTo(out, BlockPrefix.BlockType.REFERENCE, acc -> {
      Dependencies.add(fingerprint);
      Fingerprints.encode(fingerprint, acc);
      acc.writeUVLQ(position);
    });
  }

  public DurableInput.Pool underlyingBytes(IDurableCollection.Root root) {
    return root.open(fingerprint).bytes().instance().seek(position).slicePrefixedBlock().pool();
  }

  public <T extends IDurableCollection> T decodeCollection(IDurableEncoding encoding, IDurableCollection.Root root) {
    return (T) Core.decodeCollection(encoding, root.open(fingerprint), underlyingBytes(root));
  }

  public static void encode(IDurableCollection collection, DurableOutput out) {
    DurableBuffer.flushTo(out, BlockPrefix.BlockType.REFERENCE, acc -> {
      Dependencies.add(collection.root().fingerprint());
      Fingerprints.encode(collection.root().fingerprint(), acc);
      acc.writeUVLQ(collection.bytes().instance().position());
    });
  }

  public static Reference decode(DurableInput.Pool pool) {
    DurableInput in = pool.instance();

    BlockPrefix prefix = in.readPrefix();
    assert (prefix.type == BlockPrefix.BlockType.REFERENCE);

    Fingerprint fingerprint = Fingerprints.decode(in);
    long position = in.readUVLQ();
    return new Reference(fingerprint, position);
  }
}
