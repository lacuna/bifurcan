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

      @Override
      public String toString() {
        return toHexString().substring(0, 8);
      }
    };
  }

  public static byte[] trim(byte[] b, int len) {
    if (b.length == len) {
      return b;
    } else {
      byte[] result = new byte[len];
      System.arraycopy(b, 0, result, 0, len);
      return result;
    }
  }

  public static void encode(byte[] b, DurableOutput out) {
    out.writeUnsignedByte(b.length);
    out.write(b);
  }

  public static void encode(Fingerprint f, DurableOutput out) {
    encode(f.binary(), out);
  }

  public static Fingerprint decode(DurableInput in) {
    byte[] b = new byte[in.readUnsignedByte()];

    try {
      in.readFully(b);
      Fingerprint f = from(b);
      return f;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
