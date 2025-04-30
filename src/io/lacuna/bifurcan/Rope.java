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
import java.util.stream.IntStream;

import static io.lacuna.bifurcan.nodes.RopeNodes.MAX_CHUNK_CODE_UNITS;
import static java.lang.Character.isHighSurrogate;

/**
 * A tree-based immutable string representation, indexed on both full Unicode code points and Java's UTF-16 code
 * units.  Storage at the leaves uses UTF-8 encoding.  It allows for efficient {@code insert}, {@code remove},
 * {@code slice}, and {@code concat} calls, and can be converted in constant time to a Java {@code CharSequence}
 * via {@code toCharSequence()}.
 *
 * @author ztellman
 */
public class Rope implements Comparable<Rope> {

  public static final Rope EMPTY = Rope.from("");

  private final Object editor;
  private Node root;
  private int hash = -1;

  /**
   * @return a rope corresponding to {@code cs}
   */
  public static Rope from(CharSequence cs) {

    Object editor = new Object();
    Node root = new Node(editor, RopeNodes.SHIFT_INCREMENT);

    if (cs.length() > 0) {
      Iterator<byte[]> it = chunks(cs);
      while (it.hasNext()) {
        root = root.pushLast(it.next(), editor);
      }
    }

    return new Rope(root, false);

  }

  Rope(Node node, boolean linear) {
    this.editor = linear ? new Object() : null;
    this.root = node;
  }

  ///

  /**
   * @return a new Rope with {@code rope} concatenated to the end
   */
  public Rope concat(Rope rope) {
    return new Rope(root.concat(rope.root, new Object()), isLinear());
  }

  /**
   * @return the nth code point within the rope
   * @throws IndexOutOfBoundsException if {@code idx} is not within {@code [0, size)}
   */
  public int nth(int idx) {
    if (idx < 0 || idx >= size()) {
      throw new IndexOutOfBoundsException();
    }
    return root.nthPoint(idx);
  }

  /**
   * @return the number of code points in the rope
   */
  public int size() {
    return root.numCodePoints();
  }

  /**
   * @return a rope without the code points within {@code [start, end)}
   * @throws IllegalArgumentException if {@code start} or {@code end} are not within {@code [0, size) }
   */
  public Rope remove(int start, int end) {

    Object editor = isLinear() ? this.editor : new Object();

    if (end < start || start < 0 || end > size()) {
      throw new IllegalArgumentException("[" + start + ", " + end + ") is not a valid range");
    } else if (end == start) {
      return this;
    }

    // try to update a single leaf
    Node newRoot = root.update(0, start, editor, (offset, chunk) -> {
      int len = UnicodeChunk.numCodePoints(chunk);

      if (end < offset + len) {
        return UnicodeChunk.concat(
            UnicodeChunk.slice(chunk, 0, start - offset),
            UnicodeChunk.slice(chunk, end - offset, len)
        );
      } else {
        return null;
      }
    });

    if (newRoot == null) {
      newRoot = root.slice(0, start, editor).concat(root.slice(end, size(), editor), editor);
    }

    if (isLinear()) {
      root = newRoot;
      return this;
    } else {
      return new Rope(newRoot, false);
    }
  }

  private Rope insert(final int index, Iterator<byte[]> chunks, int numCodeUnits) {

    if (index < 0 || index > size()) {
      throw new IndexOutOfBoundsException();
    }

    Object editor = isLinear() ? this.editor : new Object();
    Node newRoot = null;

    // can we just update a single leaf node?
    if (numCodeUnits < MAX_CHUNK_CODE_UNITS) {
      newRoot = root.update(0, index, editor, (offset, chunk) -> {
        if (numCodeUnits + UnicodeChunk.numCodeUnits(chunk) <= MAX_CHUNK_CODE_UNITS) {
          byte[] newChunk = UnicodeChunk.slice(chunk, 0, index - offset);
          while (chunks.hasNext()) {
            newChunk = UnicodeChunk.concat(newChunk, chunks.next());
          }
          return UnicodeChunk.concat(
              newChunk,
              UnicodeChunk.slice(chunk, index - offset, UnicodeChunk.numCodePoints(chunk))
          );
        } else {
          return null;
        }
      });
    }

    if (newRoot == null) {
      newRoot = root.slice(0, index, editor);
      while (chunks.hasNext()) {
        newRoot = newRoot.pushLast(chunks.next(), editor);
      }
      newRoot = newRoot.concat(root.slice(index, size(), editor), editor);
    }

    if (isLinear()) {
      root = newRoot;
      return this;
    } else {
      return new Rope(newRoot, false);
    }
  }

  /**
   * @return a new rope with {@code rope} inserted after the first {@code idx} code points
   */
  public Rope insert(int idx, Rope rope) {
    if (rope.size() == 0) {
      return this;
    }

    return insert(idx, rope.chunks(), rope.root.numCodeUnits());

  }

  /**
   * @return a new rope with {@code cs} inserted after the first {@code index} code points
   */
  public Rope insert(int index, CharSequence cs) {
    if (cs.length() == 0) {
      return this;
    }
    return insert(index, chunks(cs), cs.length());
  }

  /**
   * @return a new rope representing the code points within {@code [start, end)}
   * @throws IllegalArgumentException if {@code end} &lt; {@code start}, or {@code start} and {@code end} are not within
   * {@code [0, size)}
   */
  public Rope slice(int start, int end) {
    if (end < start || start < 0 || end > size()) {
      throw new IllegalArgumentException("[" + start + ", " + end + ") is not a valid range");
    }

    return new Rope(root.slice(start, end, new Object()), isLinear());
  }

  public boolean isLinear() {
    return editor != null;
  }

  public Rope forked() {
    return isLinear() ? new Rope(root, false) : this;
  }

  public Rope linear() {
    return isLinear() ? this : new Rope(root, true);
  }

  /**
   * @return a sequence of bytes representing the UTF-8 encoding of the rope
   */
  public Iterator<ByteBuffer> bytes() {
    return Iterators.map(chunks(), ary -> ByteBuffer.wrap(ary, 2, ary.length - 2).slice());
  }

  /**
   * @return a sequence of integers representing the UTF-16 code units from back to front
   */
  public PrimitiveIterator.OfInt reverseChars() {
    return IntIterators.flatMap(reverseChunks(), UnicodeChunk::reverseCodeUnitIterator);
  }

  /**
   * @return a sequence of integers representing the UTF-16 code units from front to back
   */
  public PrimitiveIterator.OfInt chars() {
    return IntIterators.flatMap(chunks(), UnicodeChunk::codeUnitIterator);
  }

  /**
   * @return a sequence of integers representing the code points from back to front
   */
  public PrimitiveIterator.OfInt reverseCodePoints() {
    return IntIterators.flatMap(reverseChunks(), UnicodeChunk::reverseCodePointIterator);
  }

  /**
   * @return a sequence of integers representing the code points from front to back
   */
  public PrimitiveIterator.OfInt codePoints() {
    return IntIterators.flatMap(chunks(), UnicodeChunk::codePointIterator);
  }

  /**
   * @return a corresponding Java-style {@code String} in {@code O(N)} time
   */
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
   * @return a corresponding Java-style {@link CharSequence} in {@code O(1)} time
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
        return IntIterators.toStream(Rope.this.chars(), root.numCodeUnits());
      }

      @Override
      public IntStream codePoints() {
        return IntIterators.toStream(Rope.this.codePoints(), root.numCodePoints());
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
    };
  }

  @Override
  public int hashCode() {
    if (hash == -1) {
      hash = PerlHash.hash(0, bytes());
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Rope) {
      return Ropes.equals(this, (Rope) obj);
    } else {
      return false;
    }
  }

  /**
   * @return a value representing the lexicographic comparison of the code points
   */
  @Override
  public int compareTo(Rope o) {
    if (this == o) {
      return 0;
    } else if (size() != o.size()) {
      return size() - o.size();
    } else if (size() == 0) {
      return 0;
    } else {
      return Ropes.compare(bytes(), o.bytes());
    }
  }

  ////

  private Iterator<byte[]> reverseChunks() {
    return new Iterator<byte[]>() {

      int idx = size() - 1;

      @Override
      public boolean hasNext() {
        return idx > 0;
      }

      @Override
      public byte[] next() {
        byte[] chunk = root.chunkFor(idx);
        idx -= UnicodeChunk.numCodePoints(chunk);
        return chunk;
      }
    };
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
        int start = offset;
        int end = Math.min(cs.length(), start + MAX_CHUNK_CODE_UNITS);
        if (end < cs.length() && isHighSurrogate(cs.charAt(end - 1))) {
          end--;
        }
        offset = end;
        return UnicodeChunk.from(cs, start, end);
      }
    };
  }
}
