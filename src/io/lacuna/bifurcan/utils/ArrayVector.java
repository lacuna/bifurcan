package io.lacuna.bifurcan.utils;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class ArrayVector {
  public static Object[] create(int len) {
    return new Object[len];
  }

  public static Object[] insert(Object[] vec, int idx, Object val) {
    Object[] nVec = create(vec.length + 1);
    arraycopy(vec, 0, nVec, 0, idx);
    nVec[idx] = val;
    arraycopy(vec, idx, nVec, idx + 1, vec.length - idx);

    return nVec;
  }

  public static Object[] append(Object[] vec, Object val) {
    Object[] nVec = create(vec.length + 1);
    arraycopy(vec, 0, nVec, 0, vec.length);
    nVec[vec.length] = val;

    return nVec;
  }

  public static Object[] set(Object[] vec, int idx, Object val) {
    Object[] nVec = create(vec.length);
    arraycopy(vec, 0, nVec, 0, vec.length);
    nVec[idx] = val;

    return nVec;
  }

  public static Object[] remove(Object[] vec, int idx) {
    Object[] nVec = create(vec.length - 1);
    arraycopy(vec, 0, nVec, 0, idx);
    arraycopy(vec, idx + 1, nVec, idx, nVec.length - idx);

    return nVec;
  }
}
