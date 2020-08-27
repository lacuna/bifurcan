package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.utils.UnicodeChunk;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class RopeNodes {

  public static final int SHIFT_INCREMENT = 5;
  public static final int MAX_BRANCHES = 1 << SHIFT_INCREMENT;
  public static final int MAX_CHUNK_CODE_UNITS = MAX_BRANCHES;

  public interface ChunkUpdater {
    byte[] update(int offset, byte[] chunk);
  }

  public static Object slice(Object chunk, int start, int end, Object editor) {
    if (chunk instanceof byte[]) {
      return UnicodeChunk.slice((byte[]) chunk, start, end);
    } else {
      return ((Node) chunk).slice(start, end, editor);
    }
  }

  public static int numCodeUnits(Object node) {
    if (node instanceof byte[]) {
      return UnicodeChunk.numCodeUnits((byte[]) node);
    } else {
      return ((Node) node).numCodeUnits();
    }
  }

  public static int numCodePoints(Object node) {
    if (node instanceof byte[]) {
      return UnicodeChunk.numCodePoints((byte[]) node);
    } else {
      return ((Node) node).numCodePoints();
    }
  }

  public static Node pushLast(Node a, Object b, Object editor) {
    if (b instanceof byte[]) {
      return a.pushLast((byte[]) b, editor);
    } else {
      return a.concat((Node) b, editor);
    }
  }

  public static class Node {

    public byte shift;
    public int[] unitOffsets;
    public int[] pointOffsets;
    public Object[] nodes;
    public int numNodes;
    public final Object editor;

    // constructors

    public Node(Object editor, int shift) {
      this.shift = (byte) shift;
      this.unitOffsets = new int[2];
      this.pointOffsets = new int[2];
      this.nodes = new Object[2];
      this.numNodes = 0;
      this.editor = editor;
    }

    private Node(Object editor) {
      this.editor = editor;
    }

    private static Node from(Object editor, int shift, Node child) {
      return new Node(editor, shift).pushLast(child, editor);
    }

    private static Node from(Object editor, int shift, Node a, Node b) {
      return new Node(editor, shift).pushLast(a, editor).pushLast(b, editor);
    }

    private static Node from(Object editor, byte[] child) {
      return new Node(editor, SHIFT_INCREMENT).pushLast(child, editor);
    }

    // lookup

    private int indexFor(int idx, int[] offsets) {
      int estimate = shift > 30 ? 0 : (idx >> shift) & (MAX_BRANCHES - 1);
      for (int i = estimate; i < numNodes; i++) {
        if (idx < offsets[i]) {
          return i;
        }
      }
      throw new IndexOutOfBoundsException(idx + " is not within [0," + offsetFor(numNodes, offsets) + ")");
    }

    private static int offsetFor(int nodeIdx, int[] offsets) {
      return nodeIdx == 0 ? 0 : offsets[nodeIdx - 1];
    }

    public byte[] chunkFor(int idx) {
      Node n = this;
      for (; ; ) {
        int nodeIdx = n.indexFor(idx, n.pointOffsets);
        if (nodeIdx < 0) {
          return null;
        }

        idx -= offsetFor(nodeIdx, n.pointOffsets);
        Object o = n.nodes[nodeIdx];
        if (o instanceof Node) {
          n = (Node) o;
        } else {
          return (byte[]) o;
        }
      }
    }

    public int nthPoint(int idx) {
      Node n = this;
      for (; ; ) {
        int nodeIdx = n.indexFor(idx, n.pointOffsets);
        idx -= offsetFor(nodeIdx, n.pointOffsets);
        Object o = n.nodes[nodeIdx];
        if (o instanceof Node) {
          n = (Node) o;
        } else {
          return UnicodeChunk.nthPoint((byte[]) o, idx);
        }
      }
    }

    public char nthUnit(int idx) {
      Node n = this;
      for (; ; ) {
        int nodeIdx = n.indexFor(idx, n.unitOffsets);
        idx -= offsetFor(nodeIdx, n.unitOffsets);
        Object o = n.nodes[nodeIdx];
        if (o instanceof Node) {
          n = (Node) o;
        } else {
          return UnicodeChunk.nthUnit((byte[]) o, idx);
        }
      }
    }

    public int numCodeUnits() {
      return numNodes == 0 ? 0 : unitOffsets[numNodes - 1];
    }

    public int numCodePoints() {
      return numNodes == 0 ? 0 : pointOffsets[numNodes - 1];
    }

    // update

    public Node update(int offset, int idx, Object editor, ChunkUpdater updater) {

      if (numNodes == 0) {
        return null;
      }

      int nodeIdx = numNodes - 1;
      int nodeOffset = offsetFor(nodeIdx, pointOffsets);
      if (idx != numCodePoints()) {
        nodeIdx = indexFor(idx, pointOffsets);
        nodeOffset = offsetFor(nodeIdx, pointOffsets);
      }

      Object child = nodes[nodeIdx];
      int numUnits = RopeNodes.numCodeUnits(child);
      int numPoints = RopeNodes.numCodePoints(child);

      Object newChild = shift == SHIFT_INCREMENT
          ? updater.update(offset + nodeOffset, (byte[]) child)
          : ((Node) child).update(offset + nodeOffset, idx - nodeOffset, editor, updater);

      if (newChild == null) {
        return null;
      }

      int deltaUnits = RopeNodes.numCodeUnits(newChild) - numUnits;
      int deltaPoints = RopeNodes.numCodePoints(newChild) - numPoints;

      Node node = editor == this.editor ? this : clone(editor);
      node.nodes[nodeIdx] = newChild;

      for (int i = nodeIdx; i < numNodes; i++) {
        node.unitOffsets[i] += deltaUnits;
        node.pointOffsets[i] += deltaPoints;
      }

      return node;
    }

    public Node concat(Node node, Object editor) {

      if (node.numCodeUnits() == 0) {
        return this;
      } else if (numCodeUnits() == 0) {
        return node;
      }

      if (shift == node.shift) {
        Node newNode = this.editor == editor ? this : clone(editor);

        for (int i = 0; i < node.numNodes; i++) {
          newNode = RopeNodes.pushLast(newNode, node.nodes[i], editor);
        }

        return newNode;
      }

      if (shift < node.shift) {
        return node.pushFirst(this, editor);
      } else {
        return pushLast(node, editor);
      }
    }

    public Node slice(int start, int end, Object editor) {

      if (start == end) {
        return new Node(editor, SHIFT_INCREMENT);
      } else if (start == 0 && end == numCodePoints()) {
        return this;
      }

      int startIdx = indexFor(start, pointOffsets);
      int endIdx = indexFor(end - 1, pointOffsets);

      // we're slicing within a single node
      if (startIdx == endIdx) {
        int offset = offsetFor(startIdx, pointOffsets);
        Object child = RopeNodes.slice(nodes[startIdx], start - offset, end - offset, editor);
        if (shift > SHIFT_INCREMENT) {
          return (Node) child;
        } else {
          return from(editor, (byte[]) child);
        }

        // we're slicing across multiple nodes
      } else {

        Node newNode = new Node(editor, SHIFT_INCREMENT);

        // first partial node
        int sLower = offsetFor(startIdx, pointOffsets);
        int sUpper = offsetFor(startIdx + 1, pointOffsets);
        newNode = RopeNodes.pushLast(
            newNode,
            RopeNodes.slice(nodes[startIdx], start - sLower, sUpper - sLower, editor),
            editor
        );

        // intermediate full nodes
        for (int i = startIdx + 1; i < endIdx; i++) {
          newNode = RopeNodes.pushLast(newNode, nodes[i], editor);
        }

        // last partial node
        int eLower = offsetFor(endIdx, pointOffsets);
        return RopeNodes.pushLast(newNode, RopeNodes.slice(nodes[endIdx], 0, end - eLower, editor), editor);
      }
    }

    ///

    public Node pushLast(byte[] chunk, Object editor) {

      if (numCodeUnits() == 0 && shift > SHIFT_INCREMENT) {
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

      int numCodePoints = UnicodeChunk.numCodePoints(chunk);
      int numCodeUnits = UnicodeChunk.numCodeUnits(chunk);

      Node parent = stack[stack.length - 1];
      if (parent.nodes.length == parent.numNodes) {
        parent.grow(parent.numNodes << 2);
      }
      parent.unitOffsets[parent.numNodes] = parent.numCodeUnits();
      parent.pointOffsets[parent.numNodes] = parent.numCodePoints();
      parent.numNodes++;

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        int lastIdx = n.numNodes - 1;
        n.nodes[lastIdx] = i == stack.length - 1 ? chunk : stack[i + 1];
        n.unitOffsets[lastIdx] += numCodeUnits;
        n.pointOffsets[lastIdx] += numCodePoints;
      }

      return stack[0];
    }

    public Node pushLast(Node node, Object editor) {

      if (node.numCodeUnits() == 0) {
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
        parent.grow(parent.numNodes << 1);
      }
      parent.unitOffsets[parent.numNodes] = parent.numCodeUnits();
      parent.pointOffsets[parent.numNodes] = parent.numCodePoints();
      parent.numNodes++;

      int numCodePoints = node.numCodePoints();
      int numCodeUnits = node.numCodeUnits();

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        int lastIdx = n.numNodes - 1;
        n.nodes[lastIdx] = i == stack.length - 1 ? node : stack[i + 1];
        n.unitOffsets[lastIdx] += numCodeUnits;
        n.pointOffsets[lastIdx] += numCodePoints;
      }

      return stack[0];
    }

    public Node pushFirst(Node node, Object editor) {

      if (node.numCodeUnits() == 0) {
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
        parent.grow(parent.numNodes << 1);
      }
      arraycopy(parent.nodes, 0, parent.nodes, 1, parent.numNodes);
      arraycopy(parent.unitOffsets, 0, parent.unitOffsets, 1, parent.numNodes);
      arraycopy(parent.pointOffsets, 0, parent.pointOffsets, 1, parent.numNodes);
      parent.numNodes++;
      parent.unitOffsets[0] = 0;
      parent.pointOffsets[0] = 0;

      int numCodePoints = node.numCodePoints();
      int numCodeUnits = node.numCodeUnits();

      for (int i = 0; i < stack.length; i++) {
        Node n = stack[i];
        n.nodes[0] = i == stack.length - 1 ? node : stack[i + 1];
        for (int j = 0; j < n.numNodes; j++) {
          n.unitOffsets[j] += numCodeUnits;
          n.pointOffsets[j] += numCodePoints;
        }
      }

      return stack[0];
    }

    private void grow(int len) {
      len = Math.min(MAX_BRANCHES, len);
      int[] newUnitOffsets = new int[len];
      int[] newPointOffsets = new int[len];
      Object[] newNodes = new Object[len];

      arraycopy(unitOffsets, 0, newUnitOffsets, 0, numNodes);
      arraycopy(pointOffsets, 0, newPointOffsets, 0, numNodes);
      arraycopy(nodes, 0, newNodes, 0, numNodes);

      nodes = newNodes;
      unitOffsets = newUnitOffsets;
      pointOffsets = newPointOffsets;
    }

    public Node clone(Object editor) {

      Node n = new Node(editor);
      n.shift = shift;
      n.numNodes = numNodes;
      n.unitOffsets = unitOffsets.clone();
      n.pointOffsets = pointOffsets.clone();
      n.nodes = nodes.clone();

      return n;
    }
  }
}
