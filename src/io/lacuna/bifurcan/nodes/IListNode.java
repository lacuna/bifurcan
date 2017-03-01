package io.lacuna.bifurcan.nodes;

/**
 * @author ztellman
 */
public interface IListNode {
  IListNode slice(Object editor, int start, int end);
}
