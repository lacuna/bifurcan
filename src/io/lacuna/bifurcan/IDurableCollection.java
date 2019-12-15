package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Util;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface IDurableCollection {

  interface Fingerprint extends Comparable<Fingerprint> {
    byte[] binary();

    default String toHexString() {
      return Util.toHexString(ByteBuffer.wrap(binary()));
    }

    default int compareTo(Fingerprint o) {
      return Util.compare(
          ByteBuffer.wrap(binary()),
          ByteBuffer.wrap(o.binary()));
    }
  }

  interface Root {
    Path file();

    DurableInput bytes();

    Fingerprint fingerprint();

    IMap<Fingerprint, Root> dependencies();
  }

  IDurableEncoding encoding();

  DurableInput bytes();

  Root root();

  default IList<Fingerprint> dependencies() {
    return null;
  }

  default long rootByteOffset() {
    return bytes().bounds().absolute().start;
  }
}
