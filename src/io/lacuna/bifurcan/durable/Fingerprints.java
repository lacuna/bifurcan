package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.IDurableCollection;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.hash.PerlHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Fingerprints {

  public static Fingerprint from(byte[] binary) {
    int hash = PerlHash.hash(ByteBuffer.wrap(binary));

    return new Fingerprint() {
      @Override
      public byte[] binary() {
        return binary;
      }

      @Override
      public int hashCode() {
        return hash;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof Fingerprint) {
          return Arrays.equals(binary, ((Fingerprint) obj).binary());
        }
        return false;
      }
    };
  }

  public static void encode(byte[] b, int len, DurableOutput out) {
    out.writeUnsignedByte(len);
    out.write(b, 0, len);
  }

  public static void encode(Fingerprint f, DurableOutput out) {
    byte[] b = f.binary();
    encode(b, b.length, out);
  }

  public static Fingerprint decode(DurableInput in) {
    byte[] b = new byte[in.readUnsignedByte()];

    try {
      in.readFully(b);
      return from(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
