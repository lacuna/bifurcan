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
import static java.lang.Character.isLowSurrogate;

/**
 * A tree-based immutable string representation, indexed on both full Unicode code points and Java's UTF-16 code
 * units.  Storage at the leaves uses UTF-8 encoding.  It allows for efficient {@code insert}, {@code remove},
 * {@code slice}, and {@code concat} calls, and can be converted in constant time to a Java {@code CharSequence}
 * via {@code toCharSequence()}.
 *
 * @author ztellman
 */
public class Rope implements Comparable<Rope> {

  private final Object editor;
  public Node root;

  Rope(Object editor, Node node) {
    this.editor = editor;
    this.root = node;
  }

  /**
   * @param cs a Java-style {@code CharSequence}
   * @return a corresponding {@code Rope} representation
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

    return new Rope(editor, root);

  }

  public Rope concat(Rope rope) {
    Object editor = new Object();
    return new Rope(editor, root.concat(rope.root, editor));
  }

  public int nth(int idx) {
    return root.nthPoint(idx);
  }

  public int size() {
    return root.numCodePoints();
  }

  public Rope remove(int start, int end) {

    Object editor = new Object();

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
                UnicodeChunk.slice(chunk, end - offset, len));
      } else {
        return null;
      }
    });

    if (newRoot == null) {
      newRoot = root.slice(0, start, editor).concat(root.slice(end, size(), editor), editor);
    }
    return new Rope(editor, newRoot);
  }

  private Rope insert(final int index, Iterator<byte[]> chunks, int numCodeUnits) {

    if (index < 0 || index > size()) {
      throw new IndexOutOfBoundsException();
    }

    Object editor = new Object();

    // can we just update a single leaf node?
    if (numCodeUnits < MAX_CHUNK_CODE_UNITS) {
      Node newRoot = root.update(0, index, editor, (offset, chunk) -> {
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
        return new Rope(editor, newRoot);
      }
    }

    Node newRoot = root.slice(0, index, editor);
    while (chunks.hasNext()) {
      newRoot = newRoot.pushLast(chunks.next(), editor);
    }
    newRoot = newRoot.concat(root.slice(index, size(), editor), editor);

    return new Rope(editor, newRoot);
  }

  public Rope insert(int index, Rope rope) {
    if (rope.size() == 0) {
      return this;
    }

    return insert(index, rope.chunks(), rope.root.numCodeUnits());

  }

  public Rope insert(int index, CharSequence cs) {
    if (cs.length() == 0) {
      return this;
    }
    return insert(index, chunks(cs), cs.length());
  }

  public Rope slice(int start, int end) {
    if (end < start || start < 0 || end > size()) {
      throw new IllegalArgumentException("[" + start + ", " + end + ") is not a valid range");
    }

    Object editor = new Object();

    return new Rope(editor, root.slice(start, end, editor));
  }

  public Iterator<ByteBuffer> bytes() {
    return Iterators.map(chunks(), ary -> ByteBuffer.wrap(ary, 2, ary.length - 2).slice());
  }

  public PrimitiveIterator.OfInt reverseChars() {
    return IntIterators.flatMap(reverseChunks(), UnicodeChunk::reverseCodeUnitIterator);
  }

  public PrimitiveIterator.OfInt chars() {
    return IntIterators.flatMap(chunks(), UnicodeChunk::codeUnitIterator);
  }

  public PrimitiveIterator.OfInt reverseCodePoints() {
    return IntIterators.flatMap(reverseChunks(), UnicodeChunk::reverseCodePointIterator);
  }

  public PrimitiveIterator.OfInt codePoints() {
    return IntIterators.flatMap(chunks(), UnicodeChunk::codePointIterator);
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
        return IntIterators.toStream(Rope.this.chars(), root.numCodeUnits());
      }

      @Override
      public IntStream codePoints() {
        return IntIterators.toStream(Rope.this.codePoints(), root.numCodePoints());
      }
    };
  }

  @Override
  public int hashCode() {
    return PerlHash.hash(0, bytes());
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

  public static Iterator<byte[]> chunks(CharSequence cs) {
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
