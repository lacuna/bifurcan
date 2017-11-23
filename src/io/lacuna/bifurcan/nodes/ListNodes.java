package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.utils.Bits;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class ListNodes {

  private static final int SHIFT_INCREMENT = 5;
  private static final int MAX_BRANCHES = 1 << SHIFT_INCREMENT;
  private static final int BRANCH_MASK = MAX_BRANCHES - 1;

  public static Object slice(Object node, Object editor, int start, int end) {
    if (node instanceof Object[]) {
      Object[] ary = new Object[end - start];
      arraycopy(node, start, ary, 0, ary.length);
      return ary;
    } else {
      return ((Node) node).slice(start, end, editor);
    }
  }

  public static Object[] set(Object[] elements, int idx, Object value) {
    Object[] ary = elements.clone();
    ary[idx] = value;
    return ary;
  }

  public static Node pushLast(Node a, Object b, Object editor) {
    if (b instanceof Node) {
      return a.pushLast((Node) b, editor);
    } else {
      return a.pushLast((Object[]) b, editor);
    }
  }

  public static class Node {

    public final static Node EMPTY = new Node(new Object(), 5);

    public final byte shift;
    public boolean isStrict;
    public int numNodes;
    Object editor;
    public int[] offsets;
    public Object[] nodes;

    // constructors

    public Node(Object editor, int shift) {
      this.editor = editor;
      this.shift = (byte) shift;
      this.numNodes = 0;
      this.offsets = new int[2];
      this.nodes = new Object[2];
    }

    private Node(int shift) {
      this.shift = (byte) shift;
    }

    private static Node from(Object editor, int shift, Node child) {
      return new Node(editor, shift).pushLast(child, editor);
    }

    private static Node from(Object editor, int shift, Node a, Node b) {
      return new Node(editor, shift).pushLast(a, editor).pushLast(b, editor);
    }

    private static Node from(Object editor, Object[] child) {
      return new Node(editor, SHIFT_INCREMENT).pushLast(child, editor);
    }


    // lookup

    public Object[] first() {

      if (numNodes == 0) {
        return null;
      }

      Node n = this;
      while (n.shift > SHIFT_INCREMENT) {
        n = (Node) n.nodes[0];
      }
      return (Object[]) n.nodes[0];
    }

    public Object[] last() {

      if (numNodes == 0) {
        return null;
      }

      Node n = this;
      while (n.shift > SHIFT_INCREMENT) {
        n = (Node) n.nodes[n.numNodes - 1];
      }
      return (Object[]) n.nodes[n.numNodes - 1];
    }

    public Object nth(int idx, boolean returnChunk) {
      if (!isStrict) {
        return relaxedNth(idx, returnChunk);
      }

      Node n = this;
      while (n.shift > SHIFT_INCREMENT) {
        int nodeIdx = (idx >>> n.shift) & BRANCH_MASK;
        n = (Node) n.nodes[nodeIdx];

        if (!n.isStrict) {
          return n.relaxedNth(idx, returnChunk);
        }
      }

      Object[] chunk = (Object[]) n.nodes[(idx >>> SHIFT_INCREMENT) & BRANCH_MASK];
      return returnChunk ? chunk : chunk[idx & BRANCH_MASK];
    }

    private Object relaxedNth(int idx, boolean returnChunk) {

      // moved inside here to make nth() more inline-able
      idx = idx & (int) Bits.maskBelow(shift + SHIFT_INCREMENT);

      Node n = this;

      while (n.shift > SHIFT_INCREMENT) {
        int nodeIdx = n.indexOf(idx);
        idx -= n.offset(nodeIdx);
        n = (Node) n.nodes[nodeIdx];
      }

      int nodeIdx = n.indexOf(idx);
      Object[] chunk = (Object[]) n.nodes[nodeIdx];
      return returnChunk ? chunk : chunk[idx - n.offset(nodeIdx)];
    }

    private int indexOf(int idx) {
      int estimate = (idx >>> shift) & BRANCH_MASK;
      if (isStrict) {
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
      return idx == 0 ? 0 : offsets[idx - 1];
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

    // misc

    public int size() {
      return numNodes == 0 ? 0 : offsets[numNodes - 1];
    }

    public Node concat(Node node, Object editor) {

      if (size() == 0) {
        return node;
      } else if (node.size() == 0) {
        return this;
      }

      // same level
      if (shift == node.shift) {
        Node newNode = editor == this.editor ? this : clone(editor);

        for (int i = 0; i < node.numNodes; i++) {
          newNode = ListNodes.pushLast(newNode, node.nodes[i], editor);
        }

        return newNode;

        // we're below
      } else if (shift < node.shift) {
        return node.pushFirst(this, editor);

        // we're above
      } else {
        return pushLast(node, editor);
      }
    }

    public Node slice(int start, int end, Object editor) {

      if (start == end) {
        return EMPTY;
      } else if (start == 0 && end == size()) {
        return this;
      }

      int startIdx = indexOf(start);
      int endIdx = indexOf(end - 1);

      Node rn = new Node(editor, shift);

      // we're slicing within a single node
      if (startIdx == endIdx) {
        int offset = offset(startIdx);
        Object child = ListNodes.slice(nodes[startIdx], editor, start - offset, end - offset);
        if (shift > SHIFT_INCREMENT) {
          return (Node) child;
        } else {
          rn = ListNodes.pushLast(rn, child, editor);
        }

        // we're slicing across multiple nodes
      } else {

        // first partial node
        int sLower = offset(startIdx);
        int sUpper = offset(startIdx + 1);
        rn = ListNodes.pushLast(rn, ListNodes.slice(nodes[startIdx], editor, start - sLower, sUpper - sLower), editor);

        // intermediate full nodes
        for (int i = startIdx + 1; i < endIdx; i++) {
          rn = ListNodes.pushLast(rn, nodes[i], editor);
        }

        // last partial node
        int eLower = offset(endIdx);
        rn = ListNodes.pushLast(rn, ListNodes.slice(nodes[endIdx], editor, 0, end - eLower), editor);
      }

      return rn;
    }

    ///

    public Node pushLast(Object[] chunk, Object editor) {

      if (size() == 0 && shift > SHIFT_INCREMENT) {
        return pushLast(from(editor, chunk), editor);
      }

      Node[] stack = new Node[shift / SHIFT_INCREMENT];
      stack[0] = this;
      for (int i = 1; i < stack.length; i++) {
        Node n = stack[i - 1];
        stack[i] = (Node) n.nodes[n.numNodes - 1];
      }

      // we need to grow a parent
      if (stack[stack.length - 1].numNodes == MAX_BRANCHES) {
        return numNodes == MAX_BRANCHES
                ? new Node(editor, shift + SHIFT_INCREMENT).pushLast(this, editor).pushLast(chunk, editor)
                : pushLast(new Node(editor, SHIFT_INCREMENT).pushLast(chunk, editor), editor);
      }

      for (int i = 0; i < stack.length; i++) {
        if (stack[i].editor != editor) {
          stack[i] = stack[i].clone(editor);
        }
      }

      Node parent = stack[stack.length - 1];
      if (parent.nodes.length == parent.numNodes) {
        parent.grow();
      }
      parent.offsets[parent.numNodes] = parent.size();
      parent.numNodes++;

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        int lastIdx = n.numNodes - 1;
        n.nodes[lastIdx] = i == stack.length - 1 ? chunk : stack[i + 1];
        n.offsets[lastIdx] += chunk.length;
        n.updateStrict();
      }

      return stack[0];
    }

    public Node pushFirst(Object[] chunk, Object editor) {

      if (size() == 0 && shift > SHIFT_INCREMENT) {
        return pushLast(chunk, editor);
      }

      Node[] stack = new Node[shift / SHIFT_INCREMENT];
      stack[0] = this;
      for (int i = 1; i < stack.length; i++) {
        Node n = stack[i - 1];
        stack[i] = (Node) n.nodes[0];
      }

      // we need to grow a parent
      if (stack[stack.length - 1].numNodes == MAX_BRANCHES) {
        return numNodes == MAX_BRANCHES
                ? new Node(editor, shift + SHIFT_INCREMENT).pushLast(chunk, editor).pushLast(this, editor)
                : pushFirst(new Node(editor, SHIFT_INCREMENT).pushLast(chunk, editor), editor);
      }

      for (int i = 0; i < stack.length; i++) {
        if (stack[i].editor != editor) {
          stack[i] = stack[i].clone(editor);
        }
      }

      Node parent = stack[stack.length - 1];
      if (parent.nodes.length == parent.numNodes) {
        parent.grow();
      }
      arraycopy(parent.nodes, 0, parent.nodes, 1, parent.numNodes);
      arraycopy(parent.offsets, 0, parent.offsets, 1, parent.numNodes);
      parent.offsets[0] = 0;
      parent.numNodes++;

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        n.nodes[0] = i == stack.length - 1 ? chunk : stack[i + 1];
        for (int j = 0; j < n.numNodes; j++) {
          n.offsets[j] += chunk.length;
        }
        n.updateStrict();
      }

      return stack[0];
    }

    public Node pushLast(Node node, Object editor) {

      if (node.size() == 0) {
        return this;
      }

      if (shift < node.shift) {
        return node.pushFirst(this, editor);
      } else if (shift == node.shift) {
        return from(editor, shift + SHIFT_INCREMENT, this, node);
      }

      Node[] stack = new Node[(shift - node.shift) / SHIFT_INCREMENT];
      stack[0] = this;
      for (int i = 1; i < stack.length; i++) {
        Node n = stack[i - 1];
        stack[i] = (Node) n.nodes[n.numNodes - 1];
      }

      // we need to grow a parent
      if (stack[stack.length - 1].numNodes == MAX_BRANCHES) {
        return pushLast(from(editor, node.shift + SHIFT_INCREMENT, node), editor);
      }

      for (int i = 0; i < stack.length; i++) {
        if (stack[i].editor != editor) {
          stack[i] = stack[i].clone(editor);
        }
      }

      Node parent = stack[stack.length - 1];
      if (parent.nodes.length == parent.numNodes) {
        parent.grow();
      }
      parent.offsets[parent.numNodes] = parent.size();
      parent.numNodes++;

      int nSize = node.size();

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        int lastIdx = n.numNodes - 1;
        n.nodes[lastIdx] = i == stack.length - 1 ? node : stack[i + 1];
        n.offsets[lastIdx] += nSize;
        n.updateStrict();
      }

      return stack[0];
    }

    public Node pushFirst(Node node, Object editor) {

      if (node.size() == 0) {
        return this;
      }

      if (shift < node.shift) {
        return node.pushLast(this, editor);
      } else if (shift == node.shift) {
        return from(editor, shift + SHIFT_INCREMENT, node, this);
      }

      Node[] stack = new Node[(shift - node.shift) / SHIFT_INCREMENT];
      stack[0] = this;
      for (int i = 1; i < stack.length; i++) {
        Node n = stack[i - 1];
        stack[i] = (Node) n.nodes[0];
      }

      // we need to grow a parent
      if (stack[stack.length - 1].numNodes == MAX_BRANCHES) {
        return pushFirst(from(editor, node.shift + SHIFT_INCREMENT, node), editor);
      }

      for (int i = 0; i < stack.length; i++) {
        if (stack[i].editor != editor) {
          stack[i] = stack[i].clone(editor);
        }
      }

      Node parent = stack[stack.length - 1];
      if (parent.nodes.length == parent.numNodes) {
        parent.grow();
      }
      arraycopy(parent.nodes, 0, parent.nodes, 1, parent.numNodes);
      arraycopy(parent.offsets, 0, parent.offsets, 1, parent.numNodes);
      parent.numNodes++;
      parent.offsets[0] = 0;

      int nSize = node.size();

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        n.nodes[0] = i == stack.length - 1 ? node : stack[i + 1];
        for (int j = 0; j < n.numNodes; j++) {
          n.offsets[j] += nSize;
        }
        n.updateStrict();
      }

      return stack[0];
    }

    public Node popFirst(Object editor) {

      Node[] stack = new Node[shift / SHIFT_INCREMENT];
      stack[0] = editor == this.editor ? this : clone(editor);
      for (int i = 1; i < stack.length; i++) {
        Node n = stack[i - 1];
        stack[i] = (Node) n.nodes[0];
      }

      Node parent = stack[stack.length - 1];
      Object[] chunk = (Object[]) parent.nodes[0];

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        for (int j = 0; j < n.numNodes; j++) {
          n.offsets[j] -= chunk.length;
        }
        n.updateStrict();

        if (n.offsets[0] == 0) {

          // shift everything left
          n.numNodes--;
          if (n.numNodes == 1 && n.shift > SHIFT_INCREMENT) {
            return (Node) n.nodes[1];
          }

          arraycopy(n.nodes, 1, n.nodes, 0, n.numNodes);
          arraycopy(n.offsets, 1, n.offsets, 0, n.numNodes);
          n.nodes[n.numNodes] = null;
          n.offsets[n.numNodes] = 0;

          // no need to go any deeper
          break;
        } else {
          if (stack[i + 1].editor != editor) {
            stack[i + 1] = stack[i + 1].clone(editor);
          }
          n.nodes[0] = stack[i + 1];
        }
      }

      return stack[0];
    }

    public Node popLast(Object editor) {

      Node[] stack = new Node[shift / SHIFT_INCREMENT];
      stack[0] = editor == this.editor ? this : clone(editor);
      for (int i = 1; i < stack.length; i++) {
        Node n = stack[i - 1];
        stack[i] = (Node) n.nodes[n.numNodes - 1];
      }

      Node parent = stack[stack.length - 1];
      Object[] chunk = (Object[]) parent.nodes[parent.numNodes - 1];

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        int lastIdx = n.numNodes - 1;
        n.offsets[lastIdx] -= chunk.length;

        if (n.offset(lastIdx + 1) == n.offset(lastIdx)) {

          // lop off the rightmost node
          n.numNodes--;
          if (n.numNodes == 1 && n.shift > SHIFT_INCREMENT) {
            return (Node) n.nodes[0];
          }

          n.nodes[n.numNodes] = null;
          n.offsets[n.numNodes] = 0;
          n.updateStrict();

          // no need to go any further
          break;
        } else {
          if (stack[i + 1].editor != editor) {
            stack[i + 1] = stack[i + 1].clone(editor);
          }
          n.nodes[lastIdx] = stack[i + 1];
          n.updateStrict();
        }
      }

      return stack[0];
    }

    private void grow() {
      int[] o = new int[offsets.length << 1];
      arraycopy(offsets, 0, o, 0, offsets.length);
      this.offsets = o;

      Object[] n = new Object[nodes.length << 1];
      arraycopy(nodes, 0, n, 0, nodes.length);
      this.nodes = n;
    }

    private void updateStrict() {
      isStrict = numNodes <= 1 || offset(numNodes - 1) == (numNodes - 1) * (1 << shift);
    }

    private Node clone(Object editor) {
      Node n = new Node(shift);
      n.editor = editor;
      n.numNodes = numNodes;
      n.offsets = offsets.clone();
      n.nodes = nodes.clone();

      return n;
    }
  }
}
