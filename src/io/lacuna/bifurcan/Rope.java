package io.lacuna.bifurcan;

import io.lacuna.bifurcan.hash.PerlHash;
import io.lacuna.bifurcan.nodes.RopeNodes;
import io.lacuna.bifurcan.nodes.RopeNodes.Node;
import io.lacuna.bifurcan.utils.CharSequences;
import io.lacuna.bifurcan.utils.IntIterators;
import io.lacuna.bifurcan.utils.Iterators;
import io.lacuna.bifurcan.utils.UnicodeChunk;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static io.lacuna.bifurcan.nodes.RopeNodes.MAX_CHUNK_CODE_UNITS;
import static java.lang.Character.isLowSurrogate;

/**
 * A tree-based immutable string representation, indexed on full Unicode code points rather than Java's UTF-16 code
 * units.  Storage at the leaves uses UTF-8 encoding.  It allows for near constant-time {@code insert()} and
 * {@code remove()} calls, and can be converted in constant time to a Java {@code CharSequence}
 * via {@code toCharSequence()}.
 *
 * @author ztellman
 */
public class Rope implements ILinearizable<Rope>, IForkable<Rope>, Comparable<Rope> {

  final Object editor = new Object();
  public Node root;
  final boolean linear;

  private Rope(Node node, boolean linear) {
    this.root = node;
    this.linear = linear;
  }

  /**
   * @param cs a Java-style {@code CharSequence}
   * @return a corresponding {@code Rope} representation
   */
  public static Rope from(CharSequence cs) {
    Node root = new Node(new Object(), RopeNodes.SHIFT_INCREMENT);

    if (cs.length() == 0) {
      root.pushLast(UnicodeChunk.EMPTY);
    } else {
      chunks(cs).forEachRemaining(root::pushLast);
    }

    return new Rope(root, false);
  }

  /**
   * @return the concatenation of this rope and {@code rope}
   */
  public Rope concat(Rope rope) {
    return new Rope(root.concat(new Object(), rope.root), linear);
  }

  /**
   * @return the code point at {@code idx}
   */
  public int nth(int idx) {
    return root.nthPoint(idx);
  }

  /**
   * @return the number of code points in the rope
   */
  public int size() {
    return root.numCodePoints();
  }

  /**
   * @return a Rope without the code points within {@code [start, end)}
   */
  public Rope remove(int start, int end) {

    if (end < start || start < 0 || end > size()) {
      throw new IllegalArgumentException("[" + start + ", " + end + ") is not a valid range");
    } else if (end == start) {
      return this;
    }

    // try to update a single leaf
    Node newRoot = root.update(editor, 0, start, (offset, chunk) -> {
      int len = UnicodeChunk.numCodePoints(chunk);
      if (end < offset + len) {
        return UnicodeChunk.concat(
                UnicodeChunk.slice(chunk, 0, start - offset),
                UnicodeChunk.slice(chunk, end - offset, len));
      } else {
        return null;
      }
    });

    if (newRoot == null) {
      return slice(0, start).concat(slice(end, size()));
    } else if (linear) {
      root = newRoot;
      return this;
    } else {
      return new Rope(newRoot, false);
    }
  }

  private Rope insert(final int index, Iterator<byte[]> chunks, int numCodeUnits) {

    // can we just update a single leaf node?
    if (numCodeUnits < MAX_CHUNK_CODE_UNITS) {
      Node newRoot = root.update(editor, 0, index, (offset, chunk) -> {
        if (numCodeUnits + UnicodeChunk.numCodeUnits(chunk) <= MAX_CHUNK_CODE_UNITS) {
          byte[] newChunk = UnicodeChunk.slice(chunk, 0, index - offset);
          while (chunks.hasNext()) {
            newChunk = UnicodeChunk.concat(newChunk, chunks.next());
          }
          return UnicodeChunk.concat(newChunk, UnicodeChunk.slice(chunk, index - offset, UnicodeChunk.numCodePoints(chunk)));
        } else {
          return null;
        }
      });

      // success!
      if (newRoot != null) {
        if (linear) {
          root = newRoot;
          return this;
        } else {
          return new Rope(newRoot, false);
        }
      }
    }

    Node newRoot = root.slice(0, index);
    chunks.forEachRemaining(newRoot::pushLast);
    newRoot = newRoot.concat(editor, root.slice(index, root.numCodePoints()));

    return new Rope(newRoot, linear);
  }

  /**
   * @return inserts the code points in {@code rope} starting at {@code index}
   */
  public Rope insert(int index, Rope rope) {
    if (rope.size() == 0) {
      return this;
    }
    return insert(index, rope.chunks(), rope.root.numCodeUnits());
  }

  /**
   * @return inserts the code points in {@code cs} starting at {@code index}
   */
  public Rope insert(int index, CharSequence cs) {
    if (cs.length() == 0) {
      return this;
    }
    return insert(index, chunks(cs), cs.length());
  }

  /**
   * @return returns the code points within {@code [start, end)}
   */
  public Rope slice(int start, int end) {
    if (end < start || start < 0 || end > size()) {
      throw new IllegalArgumentException("[" + start + ", " + end + ") is not a valid range");
    }

    return new Rope(root.slice(start, end), linear);
  }

  /**
   * @return an iterator of {@code ByteBuffer} objects corresponding to a UTF-8 encoding of the rope
   */
  public Iterator<ByteBuffer> bytes() {
    return Iterators.map(chunks(), ary -> ByteBuffer.wrap(ary, 2, ary.length - 2).slice());
  }

  public PrimitiveIterator.OfInt chars() {
    Iterator<char[]> codeUnits = Iterators.map(chunks(), chunk -> {
      char[] array = new char[UnicodeChunk.numCodeUnits(chunk)];
      UnicodeChunk.writeCodeUnits(array, 0, chunk);
      return array;
    });

    return IntIterators.flatMap(codeUnits, ary -> IntIterators.range(ary.length, i -> ary[(int) i]));
  }

  public PrimitiveIterator.OfInt codePoints() {
    Iterator<int[]> codePoints = Iterators.map(chunks(), chunk -> {
      int[] array = new int[UnicodeChunk.numCodePoints(chunk)];
      UnicodeChunk.writeCodePoints(array, 0, chunk);
      return array;
    });

    return IntIterators.flatMap(codePoints, ary -> IntIterators.range(ary.length, i -> ary[(int) i]));
  }

  @Override
  public String toString() {
    char[] cs = new char[root.numCodeUnits()];
    Iterator<byte[]> it = chunks();
    int offset = 0;
    while (it.hasNext()) {
      offset += UnicodeChunk.writeCodeUnits(cs, offset, it.next());
    }

    return new String(cs);
  }

  /**
   * @return a corresponding Java-style {@code CharSequence}
   */
  public CharSequence toCharSequence() {
    return new CharSequence() {
      @Override
      public int length() {
        return root.numCodeUnits();
      }

      @Override
      public char charAt(int index) {
        return root.nthUnit(index);
      }

      @Override
      public CharSequence subSequence(int start, int end) {
        return CharSequences.subSequence(this, start, end);
      }

      @Override
      public IntStream chars() {
        return StreamSupport.intStream(Spliterators.spliterator(Rope.this.chars(), root.numCodeUnits(), Spliterator.ORDERED), false);
      }

      @Override
      public IntStream codePoints() {
        return StreamSupport.intStream(Spliterators.spliterator(Rope.this.codePoints(), root.numCodePoints(), Spliterator.ORDERED), false);
      }
    };
  }

  @Override
  public Rope forked() {
    return linear ? new Rope(root, false) : this;
  }

  @Override
  public Rope linear() {
    return linear ? this : new Rope(root, true);
  }

  @Override
  public int hashCode() {
    return PerlHash.hash(0, bytes());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Rope) {
      Rope o = (Rope) obj;
      if (size() == o.size() || root.numCodeUnits() == o.root.numCodeUnits()) {
        return size() == 0 || equals(chunks(), o.chunks());
      }
    }
    return false;
  }

  @Override
  public int compareTo(Rope o) {
    if (this == o) {
      return 0;
    }

    if (size() != o.size()) {
      return size() - o.size();
    }

    if (size() == 0) {
      return 0;
    }

    return compare(chunks(), o.chunks());
  }

  ////

  private boolean equals(Iterator<byte[]> a, Iterator<byte[]> b) {
    byte[] x = a.next();
    byte[] y = b.next();

    int i = 2;
    int j = 2;
    for (;;) {

      int len = Math.min(x.length - i, y.length - j);
      for (int k = 0; k < len; i++, j++, k++) {
        if (x[i] != y[j]) {
          return false;
        }
      }

      if (i == x.length) {
        if (!a.hasNext()) {
          break;
        }
        x = a.next();
        i = 2;
      }

      if (j == y.length) {
        if (!b.hasNext()) {
          break;
        }
        y = b.next();
        j = 2;
      }
    }

    return true;
  }

  private int compare(Iterator<byte[]> a, Iterator<byte[]> b) {

    byte[] x = a.next();
    byte[] y = b.next();

    int i = 2;
    int j = 2;
    for (;;) {

      while (i < x.length && j < y.length) {
        byte bi = x[i];
        byte bj = y[j];
        if (bi >= 0) {
          if (bi != bj) {
            return (bi & 0xFF) - (bj & 0xFF);
          }
          i++;
          j++;
        } else {
          int li = UnicodeChunk.encodedLength(bi);
          int lj = UnicodeChunk.encodedLength(bj);
          if (li != lj) {
            return li - lj;
          }

          for (int k = 0; k < li; k++, i++, j++) {
            bi = x[i];
            bj = y[j];
            if (bi != bj) {
              return (bi & 0xFF) - (bj & 0xFF);
            }
          }
        }
      }

      if (i == x.length) {
        if (!a.hasNext()) {
          break;
        }
        x = a.next();
        i = 2;
      }

      if (j == y.length) {
        if (!b.hasNext()) {
          break;
        }
        y = b.next();
        j = 2;
      }

    }

    return 0;
  }

  private Iterator<byte[]> chunks() {
    return new Iterator<byte[]>() {

      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < size();
      }

      @Override
      public byte[] next() {
        byte[] chunk = root.chunkFor(idx);
        idx += UnicodeChunk.numCodePoints(chunk);
        return chunk;
      }
    };
  }

  private static Iterator<byte[]> chunks(CharSequence cs) {
    return new Iterator<byte[]>() {
      int offset = 0;

      @Override
      public boolean hasNext() {
        return offset < cs.length();
      }

      @Override
      public byte[] next() {
        int end = Math.min(cs.length(), offset + MAX_CHUNK_CODE_UNITS);
        if (end < cs.length() && isLowSurrogate(cs.charAt(end))) {
          end--;
        }
        int start = offset;
        offset = end;
        return UnicodeChunk.from(cs, start, end);
      }
    };
  }
}
