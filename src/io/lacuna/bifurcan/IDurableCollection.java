package io.lacuna.bifurcan;

public interface IDurableCollection {

  interface Fingerprint {
    byte[] binary();

    default String toHexString() {
      StringBuffer sb = new StringBuffer();
      for (byte b : binary()) {
        sb.append(Integer.toHexString(b & 0xFF));
      }
      return sb.toString();
    }
  }

  interface Root {
    Fingerprint fingerprint();

    IList<Fingerprint> dependencies();
  }

  DurableEncoding encoding();

  DurableInput bytes();

  Root root();

  default long rootOffset() {
    return bytes().bounds().root().start;
  }
}
