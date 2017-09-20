package io.lacuna.bifurcan.utils;

/**
 * @author ztellman
 */
public class CharSequences {

  public static CharSequence subSequence(CharSequence cs, int start, int end) {
    return new CharSequence() {
      @Override
      public int length() {
        return end - start;
      }

      @Override
      public char charAt(int index) {
        return cs.charAt(start + index);
      }

      @Override
      public CharSequence subSequence(int s, int e) {
        return CharSequences.subSequence(cs, start + s, start + e);
      }

      @Override
      public String toString() {
        return cs.toString().substring(start, end);
      }
    };
  }

}
