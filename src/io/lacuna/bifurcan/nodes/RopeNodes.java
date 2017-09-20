package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.utils.UnicodeChunk;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class RopeNodes {

  public static final int SHIFT_INCREMENT = 5;
  public static final int MAX_BRANCHES = 1 << SHIFT_INCREMENT;

  public interface ChunkUpdater {
    byte[] update(int offset, byte[] chunk);
  }

  public static Object slice(Object chunk, Object editor, int start, int end) {
    if (chunk instanceof byte[]) {
      return UnicodeChunk.slice((byte[]) chunk, start, end);
    } else {
      return ((Node) chunk).slice(editor, start, end);
    }
  }

  public static int numCodeUnits(Object node) {
    return node instanceof Node ? ((Node) node).numCodeUnits() : UnicodeChunk.numCodeUnits((byte[]) node);
  }

  public static int numCodePoints(Object node) {
    return node instanceof Node ? ((Node) node).numCodePoints() : UnicodeChunk.numCodePoints((byte[]) node);
  }

  public static class Node {

    Object editor;
    public byte shift;
    public int[] unitOffsets;
    public int[] pointOffsets;
    public Object[] nodes;
    public int numNodes;

    // constructors

    public Node(Object editor, int shift) {

      if (shift > 64) {
        throw new IllegalArgumentException("excessive shift");
      }

      this.editor = editor;
      this.shift = (byte) shift;
      this.unitOffsets = new int[2];
      this.pointOffsets = new int[2];
      this.nodes = new Object[2];
      this.numNodes = 0;
    }

    private Node() {
    }

    // lookup

    private int indexFor(int idx, int[] offsets) {
      int estimate = (idx >> shift) & (MAX_BRANCHES - 1);
      for (int i = estimate; i < numNodes; i++) {
        if (idx < offsets[i]) {
          return i;
        }
      }

      throw new IndexOutOfBoundsException("could not find node for index " + idx);
    }

    private static int offsetFor(int offsetIdx, int[] offsets) {
      return offsetIdx == 0 ? 0 : offsets[offsetIdx - 1];
    }

    public byte[] chunkFor(int idx) {
      Node n = this;
      for (; ; ) {
        int nodeIdx = n.indexFor(idx, n.pointOffsets);
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

    private boolean isFull(int idx, int shift) {
      Object n = nodes[idx];
      if (n instanceof Node) {
        Node rn = (Node) n;
        return rn.shift <= shift || (rn.numNodes == MAX_BRANCHES && rn.isFull(MAX_BRANCHES - 1, shift));
      } else {
        return true;
      }
    }

    public Node addLast(Object editor, Object node) {;
      return (editor == this.editor ? this : clone(editor)).pushLast(node);
    }

    public Node addFirst(Object editor, Object node) {
      return (editor == this.editor ? this : clone(editor)).pushFirst(node);
    }

    public Node update(Object editor, int offset, int idx, ChunkUpdater updater) {

      int estimate = (idx >> shift) & (MAX_BRANCHES - 1);
      int nodeIdx = numNodes - 1;
      int nodeOffset = pointOffsets[numNodes - 1];
      for (int i = estimate; i < pointOffsets.length; i++) {
        if (idx <= pointOffsets[i]) {
          nodeIdx = i;
          nodeOffset = offsetFor(i, pointOffsets);
          break;
        }
      }

      Object child = nodes[nodeIdx];
      int numUnits = RopeNodes.numCodeUnits(child);
      int numPoints = RopeNodes.numCodePoints(child);

      Object newChild = shift == SHIFT_INCREMENT
          ? updater.update(offset + nodeOffset, (byte[]) child)
          : ((Node) child).update(editor, offset + nodeOffset, idx - nodeOffset, updater);

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

    // misc

    public Node concat(Object editor, Node node) {

      if (numCodeUnits() == 0) {
        return node;
      } else if (node.numCodeUnits() == 0) {
        return this;
      }

      // same level
      if (shift == node.shift) {
        Node newNode = clone(editor);
        for (int i = 0; i < node.numNodes; i++) {
          newNode = newNode.pushLast(node.nodes[i]);
        }
        return newNode;

        // we're down one level
      } else if (shift == node.shift - SHIFT_INCREMENT) {
        return node.addFirst(editor, this);

        // we're up one level
      } else if (shift == node.shift + SHIFT_INCREMENT) {
        return addLast(editor, node);

        // we're down multiple levels
      } else if (shift < node.shift) {
        return new Node(editor, shift + SHIFT_INCREMENT)
                .addLast(editor, this)
                .concat(editor, node);

        // we're up multiple levels
      } else {
        return concat(editor,
                new Node(editor, node.shift + SHIFT_INCREMENT)
                        .addLast(editor, node));
      }
    }

     public Node slice(Object editor, int start, int end) {

      if (start == end) {
        return new Node(new Object(), SHIFT_INCREMENT).pushLast(UnicodeChunk.EMPTY);
      }

      int startIdx = indexFor(start, pointOffsets);
      int endIdx = indexFor(end - 1, pointOffsets);

      Node rn = new Node(editor, shift);

      // we're slicing within a single node
      if (startIdx == endIdx) {
        int offset = offsetFor(startIdx, pointOffsets);
        Object child = RopeNodes.slice(nodes[startIdx], editor, start - offset, end - offset);
        if (shift > SHIFT_INCREMENT) {
          return (Node) child;
        } else {
          rn.pushLast(child);
        }

        // we're slicing across multiple nodes
      } else {

        // first partial node
        int sLower = offsetFor(startIdx, pointOffsets);
        int sUpper = offsetFor(startIdx + 1, pointOffsets);
        rn.pushLast(RopeNodes.slice(nodes[startIdx], editor, start - sLower, sUpper - sLower));

        // intermediate full nodes
        for (int i = startIdx + 1; i < endIdx; i++) {
          rn.pushLast(nodes[i]);
        }

        // last partial node
        int eLower = offsetFor(endIdx, pointOffsets);
        rn.pushLast(RopeNodes.slice(nodes[endIdx], editor, 0, end - eLower));
      }

      return rn;
    }

    ///

    public Node pushLast(Object child) {

      boolean isNode = child instanceof Node;

      if (isNode && ((Node) child).shift > shift) {
        return ((Node) child).addFirst(editor, this);
      }

      int childShift = !isNode ? 0 : ((Node) child).shift;
      boolean isFull = numNodes == 0 || isFull(numNodes - 1, shift);

      // we need to add a new level
      if (numNodes == MAX_BRANCHES && isFull) {
        return new Node(editor, shift + SHIFT_INCREMENT)
                .pushLast(this)
                .pushLast(child);
      }

      int numUnits = RopeNodes.numCodeUnits(child);
      int numPoints = RopeNodes.numCodePoints(child);

      // we need to append
      if (isFull) {

        if (childShift < shift - SHIFT_INCREMENT) {
          child = new Node(editor, shift - SHIFT_INCREMENT).pushLast(child);
        }

        int lastIdx = numNodes;
        if (numNodes == nodes.length) {
          grow(nodes.length << 1);
        }

        nodes[lastIdx] = child;
        unitOffsets[lastIdx] = offsetFor(lastIdx, unitOffsets) + numUnits;
        pointOffsets[lastIdx] = offsetFor(lastIdx, pointOffsets) + numPoints;
        numNodes++;

        // we need to go deeper
      } else {
        int lastIdx = numNodes - 1;
        nodes[lastIdx] = ((Node) nodes[lastIdx]).addLast(editor, child);
        unitOffsets[lastIdx] += numUnits;
        pointOffsets[lastIdx] += numPoints;
      }

      return this;
    }

    public Node pushFirst(Object child) {

      boolean isNode = child instanceof Node;

      if (isNode && ((Node) child).shift > shift) {
        return ((Node) child).addLast(editor, this);
      }

      int childShift = !isNode ? 0 : ((Node) child).shift;
      boolean isFull = numNodes == 0 || isFull(0, shift);

      // we need to add a new level
      if (numNodes == MAX_BRANCHES && isFull) {
        return new Node(editor, shift + SHIFT_INCREMENT).pushLast(child).pushLast(this);
      }

      int numUnits = RopeNodes.numCodeUnits(child);
      int numPoints = RopeNodes.numCodePoints(child);

      // we need to prepend
      if (isFull) {

        if (childShift < shift - SHIFT_INCREMENT) {
          child = new Node(editor, shift - SHIFT_INCREMENT).pushLast(child);
        }

        if (numNodes == nodes.length) {
          grow(nodes.length << 1);
        }

        arraycopy(nodes, 0, nodes, 1, nodes.length - 1);
        nodes[0] = child;
        for (int i = numNodes; i > 0; i--) {
          unitOffsets[i] = unitOffsets[i - 1] + numUnits;
          pointOffsets[i] = pointOffsets[i - 1] + numPoints;
        }
        unitOffsets[0] = numUnits;
        pointOffsets[0] = numPoints;
        numNodes++;

        // we need to go deeper
      } else {
        Node rn = (Node) nodes[0];
        nodes[0] = rn.addFirst(editor, child);

        for (int i = 0; i < numNodes; i++) {
          unitOffsets[i] += numUnits;
          pointOffsets[i] += numPoints;
        }
      }

      return this;
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
      Node n = new Node();
      n.editor = editor;
      n.shift = shift;
      n.unitOffsets = unitOffsets.clone();
      n.pointOffsets = pointOffsets.clone();
      n.nodes = nodes.clone();
      n.numNodes = numNodes;

      return n;
    }
  }
}
