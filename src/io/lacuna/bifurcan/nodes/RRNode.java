package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.LinearList;

import java.util.Iterator;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class RRNode {

  public final static RRNode EMPTY = new RRNode(new Object(), 5);

  public static class Leaf implements IListNode {
    public final byte size;
    public final Object[] elements;
    public final Object editor;

    public Leaf(Object editor, Object[] elements) {
      this.editor = editor;
      this.elements = elements;
      this.size = (byte) elements.length;
    }

    public Leaf set(Object editor, int idx, Object value) {
      if (editor == this.editor) {
        elements[idx] = value;
        return this;
      } else {
        Leaf leaf = new Leaf(editor, elements.clone());
        leaf.elements[idx] = value;
        return leaf;
      }
    }

    public Leaf slice(Object editor, int start, int end) {
      Object[] ary = new Object[end - start];
      arraycopy(elements, start, ary, 0, ary.length);
      return new Leaf(editor, ary);
    }

    public Leaf clone() {
      return new Leaf(editor, elements.clone());
    }
  }

  Object editor;
  public int shift;
  public int numNodes;
  public int[] offsets;
  public Object[] nodes;

  RRNode() {
  }

  public RRNode(Object editor, int shift) {
    this.editor = editor;
    this.shift = shift;
    this.numNodes = 0;
    this.offsets = new int[2];
    this.nodes = new Object[2];
  }

  void grow() {
    int[] o = new int[offsets.length << 1];
    arraycopy(offsets, 0, o, 0, offsets.length);
    this.offsets = o;

    Object[] n = new Object[nodes.length << 1];
    arraycopy(nodes, 0, n, 0, nodes.length);
    this.nodes = n;
  }

  RRNode clone(Object editor) {
    RRNode n = new RRNode();
    n.editor = editor;
    n.numNodes = numNodes;
    n.offsets = offsets.clone();
    n.nodes = nodes.clone();
    n.shift = shift;

    return n;
  }

  public boolean isFull(int idx) {
    Object n = nodes[idx];
    if (n instanceof Leaf) {
      return true;
    } else {
      RRNode rn = (RRNode) n;
      return rn.numNodes == 32 && rn.isFull(31);
    }
  }

  int indexOf(int idx) {
    for (int i = ((idx >> shift) & 31); i < nodes.length; i++) {
      if (idx < offsets[i]) {
        return i;
      }
    }
    return -1;
  }

  int offset(int idx) {
    return idx == 0 ? 0 : offsets[idx - 1];
  }

  public Object nth(int idx) {
    RRNode node = this;
    int nodeIdx = node.indexOf(idx);
    while (node.shift > 5) {
      idx -= node.offset(nodeIdx);
      node = (RRNode) node.nodes[nodeIdx];
      nodeIdx = node.indexOf(idx);
    }

    Leaf leaf = (Leaf) node.nodes[nodeIdx];
    return leaf.elements[idx - node.offset(nodeIdx)];
  }

  public int size() {
    return numNodes == 0 ? 0 : offsets[numNodes - 1];
  }

  public Leaf first() {
    if (numNodes == 0) {
      return null;
    } else if (shift == 5) {
      return (Leaf) nodes[0];
    } else {
      return ((RRNode) nodes[0]).first();
    }
  }

  public Leaf last() {
    if (numNodes == 0) {
      return null;
    } else if (shift == 5) {
      return (Leaf) nodes[numNodes - 1];
    } else {
      return ((RRNode) nodes[numNodes - 1]).first();
    }
  }

  public Iterator<Leaf> leafs() {
    LinearList list = new LinearList();
    if (size() > 0) {
      list.addLast(this);
    }

    return new Iterator<Leaf>() {
      @Override
      public boolean hasNext() {
        return list.size() > 0;
      }

      @Override
      public Leaf next() {
        while (!(list.first() instanceof Leaf)) {
          RRNode rn = (RRNode) list.first();
          list.removeFirst();
          for (int i = rn.numNodes - 1; i >= 0; i--) {
            list.addFirst(rn.nodes[i]);
          }
        }
        Leaf n = (Leaf) list.first();
        list.removeFirst();
        return n;
      }
    };
  }

  public RRNode set(Object editor, int idx, Object value) {
    return (editor == this.editor ? this : clone(editor)).overwrite(editor, idx, value);
  }

  public RRNode removeFirst(Object editor) {
    return (editor == this.editor ? this : clone(editor)).popFirst();
  }

  public RRNode removeLast(Object editor) {
    return (editor == this.editor ? this : clone(editor)).popLast();
  }

  public RRNode addLast(Object editor, Object node, int size) {
    return (editor == this.editor ? this : clone(editor)).pushLast(node, size);
  }

  public RRNode addFirst(Object editor, Object node, int size) {
    return (editor == this.editor ? this : clone(editor)).pushFirst(node, size);
  }

  public RRNode slice(Object editor, int start, int end) {
    int startIdx = indexOf(start);
    int endIdx = indexOf(end - 1);

    RRNode rn = new RRNode(editor, shift);
    if (startIdx == endIdx) {
      int offset = offset(startIdx);
      IListNode n = ((IListNode) nodes[startIdx]).slice(editor, start - offset, end - offset);
      rn.addLast(editor, n, end - start);
    } else {
      int sLower = offset(startIdx);
      int sUpper = offset(startIdx + 1);
      rn.addLast(editor, ((IListNode) nodes[startIdx]).slice(editor, start - sLower, sUpper - sLower), sUpper - start);

      for (int i = startIdx + 1; i < endIdx; i++) {
        rn.addLast(editor, nodes[i], offset(i + 1) - offset(i));
      }

      int eLower = offset(endIdx);
      rn.addLast(editor, ((IListNode) nodes[endIdx]).slice(editor, 0, end - eLower), end - eLower);
    }

    return rn;
  }

  ///

  private RRNode overwrite(Object editor, int idx, Object value) {
    int nodeIdx = indexOf(idx);
    if (shift > 5) {
      nodes[nodeIdx] = ((RRNode) nodes[nodeIdx]).set(editor, idx - offset(nodeIdx), value);
    } else {
      Leaf l = (Leaf) nodes[nodeIdx];
      nodes[nodeIdx] = l.set(editor, idx, value);
    }

    return this;
  }

  private RRNode pushLast(Object node, int size) {

    // we need to add a new level
    if (numNodes == 32 && isFull(31)) {
      return new RRNode(editor, shift + 5)
          .pushLast(this, this.size())
          .pushLast(node, size);
    }

    // we need to append
    if (numNodes == 0 || isFull(numNodes - 1)) {
      if (shift > 5 && node instanceof Leaf) {
        node = new RRNode(editor, shift - 5).pushLast(node, size);
      }

      if (numNodes == nodes.length) {
        grow();
      }

      nodes[numNodes] = node;
      offsets[numNodes] = offset(numNodes) + size;
      numNodes++;

      // we need to go deeper
    } else {
      int idx = numNodes - 1;
      RRNode rn = (RRNode) nodes[idx];
      int prevSize = rn.size();

      RRNode rnPrime = rn.addLast(editor, node, size);
      offsets[idx] += rnPrime.size() - prevSize;
      nodes[idx] = rnPrime;
    }

    return this;
  }

  private RRNode pushFirst(Object node, int size) {

    // we need to add a new level
    if (numNodes == 32 && isFull(0)) {
      return new RRNode(editor, shift + 5)
          .pushFirst(this, this.size())
          .pushFirst(node, size);
    }

    // we need to prepend
    if (numNodes == 0 || isFull(0)) {
      if (shift > 5 && node instanceof Leaf) {
        node = new RRNode(editor, shift - 5).pushLast(node, size);
      }

      if (numNodes == nodes.length) {
        grow();
      }

      arraycopy(nodes, 0, nodes, 1, numNodes);
      nodes[0] = node;
      for (int i = numNodes; i > 0; i--) {
        offsets[i] = offsets[i - 1] + size;
      }
      offsets[0] = size;
      numNodes++;

      // we need to go deeper
    } else {
      RRNode rn = (RRNode) nodes[0];
      nodes[0] = rn.addFirst(editor, node, size);
      for (int i = 0; i < numNodes; i++) {
        offsets[i] += size;
      }
    }

    return this;
  }

  private RRNode popFirst() {
    if (numNodes == 0) {
      return this;
    } else if (shift == 5) {
      numNodes--;
      int size = offsets[0];
      arraycopy(nodes, 1, nodes, 0, numNodes);
      nodes[numNodes] = null;
      for (int i = 0; i < numNodes; i++) {
        offsets[i] = offsets[i + 1] - size;
      }
      offsets[numNodes] = 0;
    } else {
      nodes[0] = ((RRNode) nodes[0]).popFirst();
    }

    return this;
  }

  private RRNode popLast() {
    if (numNodes == 0) {
      return this;
    } else if (shift == 5) {
      numNodes--;
      nodes[numNodes] = null;
      offsets[numNodes] = 0;
    } else {
      nodes[numNodes - 1] = ((RRNode) nodes[numNodes - 1]).popLast();
    }

    return this;
  }


}
