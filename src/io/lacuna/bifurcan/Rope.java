package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.RopeNodes;
import io.lacuna.bifurcan.nodes.RopeNodes.Node;
import io.lacuna.bifurcan.utils.CharSequences;
import io.lacuna.bifurcan.utils.UnicodeChunk;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static java.lang.Character.isLowSurrogate;

/**
 * @author ztellman
 */
public class Rope implements ILinearizable<Rope>, IForkable<Rope> {

  final Object editor = new Object();
  public Node root;
  final boolean linear;

  private Rope(Node node, boolean linear) {
    this.root = node;
    this.linear = linear;
  }

  public static Rope from(CharSequence cs) {
    Node root = new Node(new Object(), RopeNodes.SHIFT_INCREMENT);

    if (cs.length() == 0) {
      root.pushLast(UnicodeChunk.EMPTY);
    } else {
      chunks(cs, 1 << RopeNodes.SHIFT_INCREMENT).forEachRemaining(root::pushLast);
    }

    return new Rope(root, false);
  }

  public Rope concat(Rope rope) {
    return new Rope(root.concat(new Object(), rope.root), linear);
  }

  public int nth(int idx) {
    return root.nthPoint(idx);
  }

  public int size() {
    return root.numCodePoints();
  }

  public Rope remove(int start, int end) {

    if (end < start) {
      throw new IllegalArgumentException("arguments to 'remove' must be in-order");
    } else if (end == start) {
      return this;
    }

    Node newNode = root.update(editor, 0, start, (offset, chunk) -> {
      int len = UnicodeChunk.numCodePoints(chunk);
      if (end < offset + len) {
        return UnicodeChunk.concat(
                UnicodeChunk.slice(chunk, 0, start - offset),
                UnicodeChunk.slice(chunk, end - offset, len));
      } else {
        return null;
      }
    });

    if (newNode == null) {
      return slice(0, start).concat(slice(end, size()));
    } else if (linear) {
      root = newNode;
      return this;
    } else {
      return new Rope(newNode, false);
    }
  }

  public Rope insert(int index, CharSequence cs) {

    if (cs.length() == 0) {
      return this;
    }

    int maxLen = 1 << RopeNodes.SHIFT_INCREMENT;
    if (cs.length() < maxLen) {
      Node newNode = root.update(editor, 0, index, (offset, chunk) -> {
        if (cs.length() + UnicodeChunk.numCodeUnits(chunk) <= maxLen) {
          return UnicodeChunk.concat(
                  UnicodeChunk.slice(chunk, 0, index - offset),
                  UnicodeChunk.from(cs),
                  UnicodeChunk.slice(chunk, index - offset, UnicodeChunk.numCodePoints(chunk)));
        } else {
          return null;
        }
      });

      if (newNode != null) {
        if (linear) {
          root = newNode;
          return this;
        } else {
          return new Rope(newNode, false);
        }
      }
    }

    Object editor = new Object();
    Node newNode = root.slice(editor, 0, index);
    chunks(cs, 1 << RopeNodes.SHIFT_INCREMENT).forEachRemaining(newNode::pushLast);
    newNode = newNode.concat(editor, root.slice(editor, index, root.numCodePoints()));

    return new Rope(newNode, linear);
  }

  public Rope slice(int start, int end) {
    return new Rope(root.slice(new Object(), start, end), linear);
  }

  public Iterator<ByteBuffer> iterator() {
    return null;
  }

  @Override
  public String toString() {
    char[] cs = new char[root.numCodeUnits()];
    for (int i = 0; i < cs.length; i++) {
        cs[i] = root.nthUnit(i);
    }
    return new String(cs);
  }

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
    };
  }

  @Override
  public Rope forked() {
    return null;
  }

  @Override
  public Rope linear() {
    return null;
  }

  ////

  private static Iterator<byte[]> chunks(CharSequence cs, int chunkSize) {
    return new Iterator<byte[]>() {
      int offset = 0;

      @Override
      public boolean hasNext() {
        return offset < cs.length();
      }

      @Override
      public byte[] next() {
        int end = Math.min(cs.length(), offset + chunkSize);
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
