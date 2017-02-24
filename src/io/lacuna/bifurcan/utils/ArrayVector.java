package io.lacuna.bifurcan.utils;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class ArrayVector {

  // Object

  public static Object[] insert(Object[] vec, int idx, Object v) {
    Object[] nVec = new Object[vec.length + 1];
    arraycopy(vec, 0, nVec, 0, idx);
    nVec[idx] = v;
    arraycopy(vec, idx, nVec, idx + 1, vec.length - idx);

    return nVec;
  }

  public static Object[] insert(Object[] vec, int idx, Object v1, Object v2) {
    Object[] nVec = new Object[vec.length + 2];
    arraycopy(vec, 0, nVec, 0, idx);
    nVec[idx] = v1;
    nVec[idx + 1] = v2;
    arraycopy(vec, idx, nVec, idx + 2, vec.length - idx);

    return nVec;
  }

  public static Object[] append(Object[] vec, Object v) {
    Object[] nVec = new Object[vec.length + 1];
    arraycopy(vec, 0, nVec, 0, vec.length);
    nVec[vec.length] = v;

    return nVec;
  }

  public static Object[] append(Object[] vec, Object v1, Object v2) {
    Object[] nVec = new Object[vec.length + 2];
    arraycopy(vec, 0, nVec, 0, vec.length);
    nVec[vec.length] = v1;
    nVec[vec.length + 1] = v2;

    return nVec;
  }

  public static Object[] set(Object[] vec, int idx, Object v) {
    Object[] nVec = new Object[vec.length];
    arraycopy(vec, 0, nVec, 0, vec.length);
    nVec[idx] = v;

    return nVec;
  }

  public static Object[] set(Object[] vec, int idx, Object v1, Object v2) {
    Object[] nVec = new Object[vec.length];
    arraycopy(vec, 0, nVec, 0, vec.length);
    nVec[idx] = v1;
    nVec[idx + 1] = v2;

    return nVec;
  }

  public static Object[] remove(Object[] vec, int idx, int len) {
    Object[] nVec = new Object[vec.length - len];
    arraycopy(vec, 0, nVec, 0, idx);
    arraycopy(vec, idx + len, nVec, idx, nVec.length - idx);

    return nVec;
  }
}
