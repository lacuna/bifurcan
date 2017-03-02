package io.lacuna.bifurcan.nodes;

/**
 * @author ztellman
 */
interface IListNode {
  IListNode slice(Object editor, int start, int end);
}
