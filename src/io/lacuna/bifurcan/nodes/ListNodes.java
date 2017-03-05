package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.LinearList;

import java.util.Iterator;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class ListNodes {

  interface INode<V> {
    INode<V> slice(Object editor, int start, int end);
  }

  public static class Leaf<V> implements INode<V> {
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

  public static class Node<V> implements INode<V> {

    public final static Node EMPTY = new Node(new Object(), true, 5);

    Object editor;
    public boolean strict;
    public int shift;
    public int numNodes;
    public int[] offsets;
    public Object[] nodes;

    Node() {
    }

    public Node(Object editor, boolean strict, int shift) {
      this.strict = strict;
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

    Node clone(Object editor) {
      Node n = new Node();
      n.strict = strict;
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
        Node rn = (Node) n;
        return rn.numNodes == 32 && rn.isFull(31);
      }
    }

    int indexOf(int idx) {
      int estimate = ((idx >> shift) & 31);
      if (strict) {
        return estimate;
      }

      for (int i = estimate; i < nodes.length; i++) {
        if (idx < offsets[i]) {
          return i;
        }
      }
      return -1;
    }

    int offset(int idx) {
      return idx == 0 ? 0 : offsets[idx - 1];
    }

    private Object relaxedNth(int idx) {
      Node node = this;
      int nodeIdx = node.indexOf(idx);
      while (node.shift > 5) {
        if (node.strict) {
          return strictNth(idx);
        }
        idx -= node.offset(nodeIdx);
        node = (Node) node.nodes[nodeIdx];
        nodeIdx = node.indexOf(idx);
      }

      Leaf leaf = (Leaf) node.nodes[nodeIdx];
      return leaf.elements[idx - node.offset(nodeIdx)];
    }

    private Object strictNth(int idx) {
      Node node = this;
      while (node.shift > 5) {
        node = (Node) node.nodes[(idx >> node.shift) & 31];
        idx &= ~(31 << (node.shift + 5));
      }

      Leaf leaf = (Leaf) node.nodes[(idx >> 5) & 31];
      return leaf.elements[idx & 31];
    }

    public Object nth(int idx) {
      return strict ? strictNth(idx) : relaxedNth(idx);
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
        return ((Node) nodes[0]).first();
      }
    }

    public Leaf last() {
      if (numNodes == 0) {
        return null;
      } else if (shift == 5) {
        return (Leaf) nodes[numNodes - 1];
      } else {
        return ((Node) nodes[numNodes - 1]).first();
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
            Node rn = (Node) list.first();
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

    public Node set(Object editor, int idx, Object value) {
      return (editor == this.editor ? this : clone(editor)).overwrite(editor, idx, value);
    }

    public Node removeFirst(Object editor) {
      return (editor == this.editor ? this : clone(editor)).popFirst();
    }

    public Node removeLast(Object editor) {
      return (editor == this.editor ? this : clone(editor)).popLast();
    }

    public Node addLast(Object editor, Object node, int size) {
      return (editor == this.editor ? this : clone(editor)).pushLast(node, size);
    }

    public Node addFirst(Object editor, Object node, int size) {
      return (editor == this.editor ? this : clone(editor)).pushFirst(node, size);
    }

    public Node concat(Object editor, Node node) {

      // same level
      if (shift == node.shift) {
        return new Node(editor, false, shift + 5)
            .addLast(editor, this, this.size())
            .addLast(editor, node, node.size());

        // we're down one level
      } else if (shift == node.shift - 5) {
        return node.addFirst(editor, this, this.size());

        // we're up one level
      } else if (shift == node.shift + 5) {
        return addFirst(editor, node, node.size());

        // we're down multiple levels
      } else if (shift < node.shift) {
        return new Node(editor, false, shift + 5)
            .addLast(editor, this, this.size())
            .concat(editor, node);

        // we're up multiple levels
      } else {
        return concat(editor,
            new Node(editor, false, node.shift + 5)
                .addLast(editor, node, node.size()));
      }
    }

    public Node slice(Object editor, int start, int end) {
      int startIdx = indexOf(start);
      int endIdx = indexOf(end - 1);

      Node rn = new Node(editor, false, shift);

      // we're slicing within a single node
      if (startIdx == endIdx) {
        int offset = offset(startIdx);
        INode n = ((INode) nodes[startIdx]).slice(editor, start - offset, end - offset);
        rn.addLast(editor, n, end - start);

        // we're slicing across multiple nodes
      } else {

        // first partial node
        int sLower = offset(startIdx);
        int sUpper = offset(startIdx + 1);
        rn.addLast(editor, ((INode) nodes[startIdx]).slice(editor, start - sLower, sUpper - sLower), sUpper - start);

        // intermediate full nodes
        for (int i = startIdx + 1; i < endIdx; i++) {
          rn.addLast(editor, nodes[i], offset(i + 1) - offset(i));
        }

        // last partial node
        int eLower = offset(endIdx);
        rn.addLast(editor, ((INode) nodes[endIdx]).slice(editor, 0, end - eLower), end - eLower);
      }

      return rn;
    }

    ///

    private Node overwrite(Object editor, int idx, Object value) {
      int nodeIdx = indexOf(idx);
      if (shift > 5) {
        nodes[nodeIdx] = ((Node) nodes[nodeIdx]).set(editor, idx - offset(nodeIdx), value);
      } else {
        Leaf l = (Leaf) nodes[nodeIdx];
        nodes[nodeIdx] = l.set(editor, idx, value);
      }

      return this;
    }

    private Node pushLast(Object node, int size) {

      // we need to add a new level
      if (numNodes == 32 && isFull(31)) {
        return new Node(editor, strict, shift + 5)
            .pushLast(this, this.size())
            .pushLast(node, size);
      }

      // we need to append
      if (numNodes == 0 || isFull(numNodes - 1)) {
        if (shift > 5 && node instanceof Leaf) {
          node = new Node(editor, true, shift - 5).pushLast(node, size);
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
        nodes[idx] = ((Node) nodes[idx]).addLast(editor, node, size);
        offsets[idx] += size;
      }

      return this;
    }

    private Node pushFirst(Object node, int size) {

      // we need to add a new level
      if (numNodes == 32 && isFull(0)) {
        return new Node(editor, false, shift + 5)
            .pushFirst(this, this.size())
            .pushFirst(node, size);
      }

      // we need to prepend
      if (numNodes == 0 || isFull(0)) {
        if (shift > 5 && node instanceof Leaf) {
          node = new Node(editor, false, shift - 5).pushLast(node, size);
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
        Node rn = (Node) nodes[0];
        nodes[0] = rn.addFirst(editor, node, size);

        for (int i = 0; i < numNodes; i++) {
          offsets[i] += size;
        }
      }

      strict = false;
      return this;
    }

    private Node popFirst() {
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
        Node rn = (Node) nodes[0];
        int prevSize = rn.size();
        Node rnPrime = rn.popFirst();
        int delta = rnPrime.size() - prevSize;

        nodes[0] = rnPrime;
        for (int i = 0; i < numNodes; i++) {
          offsets[i] += delta;
        }
      }

      strict = false;
      return this;
    }

    private Node popLast() {
      if (numNodes == 0) {
        return this;

      } else if (shift == 5) {
        numNodes--;
        nodes[numNodes] = null;
        offsets[numNodes] = 0;

      } else {
        Node rn = (Node) nodes[numNodes - 1];
        int prevSize = rn.size();
        Node rnPrime = rn.popLast();

        nodes[numNodes - 1] = rnPrime;
        offsets[numNodes - 1] += rnPrime.size() - prevSize;
      }

      return this;
    }


  }
}
