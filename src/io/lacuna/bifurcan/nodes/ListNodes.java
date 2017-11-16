package io.lacuna.bifurcan.nodes;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class ListNodes {

  private static final int SHIFT_INCREMENT = 5;
  private static final int MAX_BRANCHES = 1 << SHIFT_INCREMENT;

  public static Object slice(Object node, Object editor, int start, int end) {
    if (node instanceof Object[]) {
      Object[] ary = new Object[end - start];
      arraycopy(node, start, ary, 0, ary.length);
      return ary;
    } else {
      return ((Node) node).slice(editor, start, end);
    }
  }

  public static Object[] set(Object[] elements, int idx, Object value) {
    Object[] ary = elements.clone();
    ary[idx] = value;
    return ary;
  }

  public static int nodeSize(Object node) {
    return node instanceof Node ? ((Node) node).size() : ((Object[]) node).length;
  }

  public static class Node {

    public final static Node EMPTY = new Node(new Object(), true, 5);

    Object editor;
    public boolean strict;
    public byte shift;
    public int numNodes;
    public int[] offsets;
    public Object[] nodes;

    // constructors

    public Node(Object editor, boolean strict, int shift) {
      this.strict = strict;
      this.editor = editor;
      this.shift = (byte) shift;
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
      } else if (shift == SHIFT_INCREMENT) {
        return (Object[]) nodes[0];
      } else {
        return ((Node) nodes[0]).first();
      }
    }

    public Object[] last() {
      if (numNodes == 0) {
        return null;
      } else if (shift == SHIFT_INCREMENT) {
        return (Object[]) nodes[numNodes - 1];
      } else {
        return ((Node) nodes[numNodes - 1]).last();
      }
    }

    private boolean isFull(int idx, int shift) {
      Object n = nodes[idx];
      if (n instanceof Node) {
        Node rn = (Node) n;
        return shift >= rn.shift || (rn.numNodes == MAX_BRANCHES && rn.isFull(MAX_BRANCHES - 1, shift));
      } else {
        return true;
      }
    }

    private Object[] strictArrayFor(int idx) {
      Node n = this;
      while (n.shift > SHIFT_INCREMENT) {
        n = (Node) n.nodes[(idx >>> n.shift) & (MAX_BRANCHES - 1)];
      }
      return (Object[]) n.nodes[(idx >>> SHIFT_INCREMENT) & (MAX_BRANCHES - 1)];
    }

    private Object[] relaxedArrayFor(int idx) {
      Node n = this;
      while (n.shift > SHIFT_INCREMENT) {
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
      while (n.shift > SHIFT_INCREMENT) {
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
      return strictArrayFor(idx)[idx & (MAX_BRANCHES - 1)];
    }

    private int indexOf(int idx) {
      int estimate = (idx >> shift) & (MAX_BRANCHES - 1);
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
        return (32 << (shift - SHIFT_INCREMENT)) * idx;
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
      if (shift == SHIFT_INCREMENT) {
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

    public Node addLast(Object editor, Object chunk) {
      return (editor == this.editor ? this : clone(editor)).pushLast(chunk);
    }

    public Node addFirst(Object editor, Object chunk) {
      return (editor == this.editor ? this : clone(editor)).pushFirst(chunk);
    }

    // misc

    public int size() {
      return numNodes == 0 ? 0 : offsets[numNodes - 1];
    }

    public Node concat(Object editor, Node node) {

      if (size() == 0) {
        return node;
      } else if (node.size() == 0) {
        return this;
      }

      // same level
      if (shift == node.shift) {
        Node newNode = clone(editor);
        for (int i = 0; i < node.numNodes; i++) {
          newNode = newNode.pushLast(node.nodes[i]);
        }
        return newNode;

        // we're below
      } else if (shift < node.shift) {
        return node.addFirst(editor, this);

        // we're above
      } else {
        return addLast(editor, node);
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
        Object child = ListNodes.slice(nodes[startIdx], editor, start - offset, end - offset);
        if (shift > SHIFT_INCREMENT) {
          return (Node) child;
        } else {
          rn.pushLast(child);
        }

        // we're slicing across multiple nodes
      } else {

        // first partial node
        int sLower = offset(startIdx);
        int sUpper = offset(startIdx + 1);
        rn.pushLast(ListNodes.slice(nodes[startIdx], editor, start - sLower, sUpper - sLower));

        // intermediate full nodes
        for (int i = startIdx + 1; i < endIdx; i++) {
          rn.pushLast(nodes[i]);
        }

        // last partial node
        int eLower = offset(endIdx);
        rn.pushLast(ListNodes.slice(nodes[endIdx], editor, 0, end - eLower));
      }

      return rn;
    }

    ///

    private Node pushLast(Object child) {

      boolean isNode = child instanceof Node;

      if (isNode && ((Node) child).shift > shift) {
        return ((Node) child).addFirst(editor, this);
      }

      int childShift = !isNode ? 0 : ((Node) child).shift;
      boolean isFull = numNodes == 0 || isFull(numNodes - 1, childShift);

      // we need to add a new level
      if (numNodes == MAX_BRANCHES && isFull) {
        return new Node(editor, strict, shift + SHIFT_INCREMENT).pushLast(this).pushLast(child);
      }

      int size = nodeSize(child);

      // we need to append
      if (isFull) {

        strict = strict && ((!isNode && nodeSize(child) == MAX_BRANCHES) || (isNode && ((Node) child).strict));

        if (childShift < shift - SHIFT_INCREMENT) {
          child = new Node(editor, true, shift - SHIFT_INCREMENT).pushLast(child);
        }

        if (numNodes == nodes.length) {
          grow();
        }

        nodes[numNodes] = child;
        offsets[numNodes] = offset(numNodes) + size;
        numNodes++;

        // we need to go deeper
      } else {
        int idx = numNodes - 1;
        nodes[idx] = ((Node) nodes[idx]).addLast(editor, child);
        offsets[idx] += size;
      }

      return this;
    }

    private Node pushFirst(Object child) {

      boolean isNode = child instanceof Node;

      if (isNode && ((Node) child).shift > shift) {
        return ((Node) child).addLast(editor, this);
      }

      int childShift = !isNode ? 0 : ((Node) child).shift;
      boolean isFull = numNodes == 0 || isFull(0, childShift);

      // we need to add a new level
      if (numNodes == MAX_BRANCHES && isFull) {
        return new Node(editor, false, shift + SHIFT_INCREMENT).pushLast(child).pushLast(this);
      }

      int size = nodeSize(child);

      // we need to prepend
      if (isFull) {

        strict = strict && ((!isNode && nodeSize(child) == MAX_BRANCHES) || (isFull && isNode && ((Node) child).strict));

        if (childShift < shift - SHIFT_INCREMENT) {
          child = new Node(editor, false, shift - SHIFT_INCREMENT).pushLast(child);
        }

        if (numNodes == nodes.length) {
          grow();
        }

        arraycopy(nodes, 0, nodes, 1, numNodes);
        nodes[0] = child;
        for (int i = numNodes; i > 0; i--) {
          offsets[i] = offsets[i - 1] + size;
        }
        offsets[0] = size;
        numNodes++;

        // we need to go deeper
      } else {
        Node rn = (Node) nodes[0];
        nodes[0] = rn.addFirst(editor, child);

        for (int i = 0; i < numNodes; i++) {
          offsets[i] += size;
        }
      }

      strict = false;
      return this;
    }

    private Node popFirst() {
      int delta = 0;
      boolean shiftLeft = false;
      if (numNodes == 0) {
        return this;

      } else if (shift == SHIFT_INCREMENT) {
        delta = -offsets[0];
        shiftLeft = true;

      } else {
        Node rn = (Node) nodes[0];
        int prevSize = rn.size();
        Node rnPrime = rn.popFirst();
        delta = rnPrime.size() - prevSize;
        shiftLeft = rnPrime.size() == 0;

        nodes[0] = rnPrime;
      }

      if (shiftLeft) {
        numNodes--;
        arraycopy(nodes, 1, nodes, 0, numNodes);
        nodes[numNodes] = null;
        for (int i = 0; i < numNodes; i++) {
          offsets[i] = offsets[i + 1] + delta;
        }
        offsets[numNodes] = 0;
      } else {
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

      } else if (shift == SHIFT_INCREMENT) {
        numNodes--;
        nodes[numNodes] = null;
        offsets[numNodes] = 0;

      } else {
        Node rn = (Node) nodes[numNodes - 1];
        int prevSize = rn.size();
        Node rnPrime = rn.popLast();

        if (rnPrime.size() == 0) {
          numNodes--;
          nodes[numNodes] = null;
          offsets[numNodes] = 0;
        } else {
          nodes[numNodes - 1] = rnPrime;
          offsets[numNodes - 1] += rnPrime.size() - prevSize;
        }
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
