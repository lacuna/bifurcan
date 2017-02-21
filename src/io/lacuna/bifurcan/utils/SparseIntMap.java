package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.Lists;
import io.lacuna.bifurcan.Maps;

import java.util.Optional;

/**
 * @author ztellman
 */
public class SparseIntMap<V> {

  public static final SparseIntMap EMPTY = new SparseIntMap((byte) 0, new long[0], new Object[0]);

  public final byte keyBits;
  public final long[] keys;
  public final Object[] vals;

  private SparseIntMap(byte keyBits, long[] keys, Object[] vals) {
    this.keyBits = keyBits;
    this.keys = keys;
    this.vals = vals;
  }

  public int size() {
    return vals.length;
  }

  public long key(int idx) {
    return BitVector.get(keys, idx * keyBits, keyBits);
  }

  public V val(int idx) {
    return (V) vals[idx];
  }

  public Optional<V> get(long key) {
    int idx = BitIntSet.indexOf(keys, keyBits, vals.length, key);
    if (idx < 0) {
      return Optional.empty();
    } else {
      return Optional.of((V) val(idx));
    }
  }

  private byte minBitLength(long val) {
    return (byte) (Bits.log2Floor(val) + 1);
  }

  public IList<V> values() {
    return Lists.from(vals.length, i -> (V) vals[(int) i]);
  }

  private SparseIntMap<V> append(long key, V val) {
    Object[] nVals = ArrayVector.append(vals, val);

    byte nKeyBits = minBitLength(key);
    if (keyBits == nKeyBits) {
      int bitLength = vals.length * keyBits;
      return new SparseIntMap<V>(
              keyBits,
              BitVector.insert(keys, bitLength, key, bitLength, keyBits),
              nVals);
    } else {
      int limit = nKeyBits * vals.length;
      long[] nKeys = BitVector.create(nKeyBits * (vals.length + 1));
      for (int i = 0, j = 0; j < limit; i += keyBits, j += nKeyBits) {
        BitVector.overwrite(nKeys, BitVector.get(keys, i, keyBits), j, nKeyBits);
      }
      BitVector.overwrite(nKeys, key, limit, nKeyBits);

      return new SparseIntMap<V>(nKeyBits, nKeys, nVals);
    }
  }

  private SparseIntMap<V> unappend() {

    if (size() == 1) {
      return EMPTY;
    }

    byte nKeyBits = minBitLength(key(size() - 2));
    Object[] nVals = ArrayVector.remove(vals, size() - 1, 1);

    if (keyBits == nKeyBits) {
      int bitLength = vals.length * keyBits;
      return new SparseIntMap<V>(
              keyBits,
              BitVector.remove(keys, bitLength, bitLength - keyBits, keyBits),
              nVals);
    } else {
      int limit = nKeyBits * (vals.length - 1);
      long[] nKeys = BitVector.create(nKeyBits * (vals.length + 1));
      for (int i = 0, j = 0; j < limit; i += keyBits, j += nKeyBits) {
        BitVector.overwrite(nKeys, BitVector.get(keys, i, keyBits), j, nKeyBits);
      }

      return new SparseIntMap<V>(nKeyBits, nKeys, nVals);
    }
  }

  public IMap.IEntry<Long, V> floorEntry(long key) {
    int idx = BitIntSet.indexOf(keys, keyBits, vals.length, key);
    System.out.println(key + " " + idx);
    idx = idx < 0 ? -idx - 2 : idx;
    return idx < 0 ? null : new Maps.Entry<>(BitIntSet.get(keys, keyBits, idx), (V) vals[idx]);
  }

  public SparseIntMap<V> put(long key, V val) {
    int idx = BitIntSet.indexOf(keys, keyBits, vals.length, key);
    if (idx == -(size() + 1)) {
      return append(key, val);
    } else if (idx < 0) {
      idx = -idx - 1;
      return new SparseIntMap<V>(
              keyBits,
              BitVector.insert(keys, (vals.length * keyBits), key, (idx * keyBits), keyBits),
              ArrayVector.insert(vals, idx, val));
    } else {
      return new SparseIntMap<V>(
              keyBits,
              BitVector.clone(keys),
              ArrayVector.set(vals, idx, val));
    }
  }

  public SparseIntMap<V> remove(long key) {
    int idx = BitIntSet.indexOf(keys, keyBits, vals.length, key);
    if (idx < 0) {
      return this;
    } else if (idx == (size() - 1)) {
      return unappend();
    } else {
      return new SparseIntMap<V>(
              keyBits,
              BitVector.remove(keys, (keyBits * vals.length), (keyBits * idx), keyBits),
              ArrayVector.remove(vals, idx, 1));
    }
  }

}
