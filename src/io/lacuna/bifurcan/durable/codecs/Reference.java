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

  public static Reference from(IDurableCollection collection) {
    return new Reference(collection.root().fingerprint(), collection.bytes().instance().position());
  }

  public void encode(DurableOutput out) {
    DurableBuffer.flushTo(out, BlockPrefix.BlockType.REFERENCE, acc -> {
      Dependencies.add(fingerprint);
      Fingerprints.encode(fingerprint, acc);
      acc.writeUVLQ(position);
    });
  }

  public IDurableCollection decodeCollection(IDurableEncoding encoding, IDurableCollection.Root root) {
    IDurableCollection.Root underlyingRoot = root.open(fingerprint);
    DurableInput.Pool underlyingBytes = underlyingRoot.bytes().instance().seek(position).slicePrefixedBlock().pool();
    return Util.decodeCollection(encoding, underlyingRoot, underlyingBytes);
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
