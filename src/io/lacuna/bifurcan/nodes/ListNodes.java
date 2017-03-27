package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.LinearList;

import java.util.Iterator;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class ListNodes {

  public static Object slice(Object chunk, Object editor, int start, int end) {
    if (chunk instanceof Object[]) {
      Object[] ary = new Object[end - start];
      arraycopy(chunk, start, ary, 0, ary.length);
      return ary;
    } else {
      return ((Node) chunk).slice(editor, start, end);
    }
  }

  public static Object[] set(Object[] elements, int idx, Object value) {
    Object[] ary = elements.clone();
    ary[idx] = value;
    return ary;
  }

  public static class Node {

    public final static Node EMPTY = new Node(new Object(), true, 5);

    Object editor;
    public boolean strict;
    public int shift;
    public int numNodes;
    public int[] offsets;
    public Object[] nodes;

    // constructors

    public Node(Object editor, boolean strict, int shift) {
      this.strict = strict;
      this.editor = editor;
      this.shift = shift;
      this.numNodes = 0;
      this.offsets = new int[2];
      this.nodes = new Object[2];
    }

    private Node() {
    }

    // lookup

    public Object nth(int idx) {
      return strict ? strictNth(idx) : relaxedNth(idx);
    }

    public Object[] arrayFor(int idx) {
      return strict ? strictArrayFor(idx) : relaxedArrayFor(idx);
    }

    public Object[] first() {
      if (numNodes == 0) {
        return null;
      } else if (shift == 5) {
        return (Object[]) nodes[0];
      } else {
        return ((Node) nodes[0]).first();
      }
    }

    public Object[] last() {
      if (numNodes == 0) {
        return null;
      } else if (shift == 5) {
        return (Object[]) nodes[numNodes - 1];
      } else {
        return ((Node) nodes[numNodes - 1]).first();
      }
    }

    private boolean isFull(int idx) {
      Object n = nodes[idx];
      if (n instanceof Node) {
        Node rn = (Node) n;
        return rn.numNodes == 32 && rn.isFull(31);
      } else {
        return true;
      }
    }

    private Object[] strictArrayFor(int idx) {
      Node n = this;
      while (n.shift > 5) {
        n = (Node) n.nodes[(idx >>> n.shift) & 31];
      }
      return (Object[]) n.nodes[(idx >>> 5) & 31];
    }

    private Object[] relaxedArrayFor(int idx) {
      Node n = this;
      while (n.shift > 5) {
        if (n.strict) {
          return n.strictArrayFor(idx);
        }
        int nodeIdx = n.indexOf(idx);
        idx -= n.offset(nodeIdx);
        n = (Node) n.nodes[nodeIdx];
      }
      return (Object[]) n.nodes[n.indexOf(idx)];
    }

    private Object relaxedNth(int idx) {
      Node n = this;
      while (n.shift > 5) {
        if (n.strict) {
          return n.strictNth(idx);
        }
        int nodeIdx = n.indexOf(idx);
        idx -= n.offset(nodeIdx);
        n = (Node) n.nodes[nodeIdx];
      }

      int nodeIdx = n.indexOf(idx);
      return ((Object[]) n.nodes[nodeIdx])[idx - n.offset(nodeIdx)];
    }

    private Object strictNth(int idx) {
      return strictArrayFor(idx)[idx & 31];
    }

    private int indexOf(int idx) {
      int estimate = (idx >> shift) & 31;
      if (strict) {
        return estimate;
      } else {
        for (int i = estimate; i < nodes.length; i++) {
          if (idx < offsets[i]) {
            return i;
          }
        }
        return -1;
      }
    }

    int offset(int idx) {
      if (strict) {
        return (32 << (shift - 5)) * idx;
      } else {
        return idx == 0 ? 0 : offsets[idx - 1];
      }
    }

    // update

    public Node set(Object editor, int idx, Object value) {
      if (editor != this.editor) {
        return clone(editor).set(editor, idx, value);
      }

      int nodeIdx = indexOf(idx);
      if (shift == 5) {
        nodes[nodeIdx] = ListNodes.set((Object[]) nodes[nodeIdx], idx - offset(nodeIdx), value);
      } else {
        nodes[nodeIdx] = ((Node) nodes[nodeIdx]).set(editor, idx - offset(nodeIdx), value);
      }
      return this;
    }

    public Node removeFirst(Object editor) {
      return (editor == this.editor ? this : clone(editor)).popFirst();
    }

    public Node removeLast(Object editor) {
      return (editor == this.editor ? this : clone(editor)).popLast();
    }

    public Node addLast(Object editor, Object chunk, int size) {
      return (editor == this.editor ? this : clone(editor)).pushLast(chunk, size);
    }

    public Node addFirst(Object editor, Object chunk, int size) {
      return (editor == this.editor ? this : clone(editor)).pushFirst(chunk, size);
    }

    // misc

    public int size() {
      return numNodes == 0 ? 0 : offsets[numNodes - 1];
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

      if (start == end) {
        return EMPTY;
      }

      int startIdx = indexOf(start);
      int endIdx = indexOf(end - 1);

      Node rn = new Node(editor, false, shift);

      // we're slicing within a single node
      if (startIdx == endIdx) {
        int offset = offset(startIdx);
        Object n = ListNodes.slice(nodes[startIdx], editor, start - offset, end - offset);
        rn.addLast(editor, n, end - start);

        // we're slicing across multiple nodes
      } else {

        // first partial node
        int sLower = offset(startIdx);
        int sUpper = offset(startIdx + 1);
        rn.addLast(editor, ListNodes.slice(nodes[startIdx], editor, start - sLower, sUpper - sLower), sUpper - start);

        // intermediate full nodes
        for (int i = startIdx + 1; i < endIdx; i++) {
          rn.addLast(editor, nodes[i], offset(i + 1) - offset(i));
        }

        // last partial node
        int eLower = offset(endIdx);
        rn.addLast(editor, ListNodes.slice(nodes[endIdx], editor, 0, end - eLower), end - eLower);
      }

      return rn;
    }

    ///

    private Node pushLast(Object chunk, int size) {

      boolean isNode = chunk instanceof Node;

      if (isNode && ((Node) chunk).shift > shift) {
        return ((Node) chunk).addFirst(editor, this, size());
      }

      // we need to add a new level
      if (numNodes == 32 && isFull(31)) {
        return new Node(editor, strict, shift + 5)
            .pushLast(this, this.size())
            .pushLast(chunk, size);
      }

      boolean isFull = numNodes == 0 || isFull(numNodes - 1);

      // we need to append
      if (isFull || (isNode && ((Node) chunk).shift == (shift - 5))) {

        strict = strict && ((!isNode && size == 32) || (isFull && isNode && ((Node) chunk).strict));

        if (shift > 5 && !isNode) {
          chunk = new Node(editor, true, shift - 5).pushLast(chunk, size);
        }

        if (numNodes == nodes.length) {
          grow();
        }

        nodes[numNodes] = chunk;
        offsets[numNodes] = offset(numNodes) + size;
        numNodes++;

        // we need to go deeper
      } else {
        int idx = numNodes - 1;
        nodes[idx] = ((Node) nodes[idx]).addLast(editor, chunk, size);
        offsets[idx] += size;
      }

      return this;
    }

    private Node pushFirst(Object chunk, int size) {

      boolean isNode = chunk instanceof Node;

      if (isNode && ((Node) chunk).shift > shift) {
        return ((Node) chunk).addLast(editor, this, size());
      }

      // we need to add a new level
      if (numNodes == 32 && isFull(0)) {
        return new Node(editor, false, shift + 5)
            .pushFirst(this, this.size())
            .pushFirst(chunk, size);
      }

      boolean isFull = numNodes == 0 || isFull(0);

      // we need to prepend
      if (isFull || (isNode && ((Node) chunk).shift == (shift - 5))) {

        strict = strict && ((!isNode && size == 32) || (isFull && isNode && ((Node) chunk).strict));

        if (shift > 5 && !isNode) {
          chunk = new Node(editor, false, shift - 5).pushLast(chunk, size);
        }

        if (numNodes == nodes.length) {
          grow();
        }

        arraycopy(nodes, 0, nodes, 1, numNodes);
        nodes[0] = chunk;
        for (int i = numNodes; i > 0; i--) {
          offsets[i] = offsets[i - 1] + size;
        }
        offsets[0] = size;
        numNodes++;

        // we need to go deeper
      } else {
        Node rn = (Node) nodes[0];
        nodes[0] = rn.addFirst(editor, chunk, size);

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

    private void grow() {
      int[] o = new int[offsets.length << 1];
      arraycopy(offsets, 0, o, 0, offsets.length);
      this.offsets = o;

      Object[] n = new Object[nodes.length << 1];
      arraycopy(nodes, 0, n, 0, nodes.length);
      this.nodes = n;
    }

    private Node clone(Object editor) {
      Node n = new Node();
      n.strict = strict;
      n.editor = editor;
      n.numNodes = numNodes;
      n.offsets = offsets.clone();
      n.nodes = nodes.clone();
      n.shift = shift;

      return n;
    }
  }
}
