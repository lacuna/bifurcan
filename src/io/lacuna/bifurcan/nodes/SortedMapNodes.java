package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BinaryOperator;

import static io.lacuna.bifurcan.nodes.SortedMapNodes.Color.*;

public class SortedMapNodes {

  public static final Node EMPTY_NODE = new Node(BLACK, null, null, null, null);
  public static final Node DOUBLE_EMPTY_NODE = new Node(DOUBLE_BLACK, null, null, null, null);

  public enum Color {
    RED,
    BLACK,
    DOUBLE_BLACK
  }

  public static class Node<K, V> {
    public final Color c;
    public final K k;
    public final V v;
    public final Node<K, V> l, r;
    public final int size;

    public Node(Color c, Node<K, V> l, K k, V v, Node<K, V> r) {
      this.c = c;
      this.k = k;
      this.v = v;
      this.l = l;
      this.r = r;

      this.size = l == null ? 0 : l.size + r.size + 1;
    }

    public Node<K, V> redden() {
      return c == BLACK && size > 0 && l.c == BLACK && r.c == BLACK
        ? node(RED, l, k, v, r)
        : this;
    }

    public Node<K, V> blacken() {
      return c == RED ? node(BLACK, l, k, v, r) : this;
    }

    public Node<K, V> unblacken() {
      return c == DOUBLE_BLACK ? node(BLACK, l, k, v, r) : this;
    }

    public Node<K, V> remove(K key, Comparator<K> comparator) {
      return redden()._remove(key, comparator);
    }

    private Node<K, V> _remove(K key, Comparator<K> comparator) {
      if (size == 0) {
        return this;
      } else {
        int cmp = comparator.compare(key, k);
        if (cmp < 0) {
          return node(c, l._remove(key, comparator), k, v, r).rotate();
        } else if (cmp > 0) {
          return node(c, l, k, v, r._remove(key, comparator)).rotate();
        } else if (size == 1) {
          return c == BLACK ? DOUBLE_EMPTY_NODE : EMPTY_NODE;
        } else if (r.size == 0) {
          return l.blacken();
        } else {
          Node<K, V> min = min(r);
          return node(c, l, min.k, min.v, r.removeMin()).rotate();
        }
      }
    }

    public Node<K, V> put(K key, V value, BinaryOperator<V> merge, Comparator<K> comparator) {
      return _put(key, value, merge, comparator).blacken();
    }

    private Node<K, V> _put(K key, V value, BinaryOperator<V> merge, Comparator<K> comparator) {
      if (size == 0) {
        return node(c == DOUBLE_BLACK ? BLACK : RED, EMPTY_NODE, key, value, EMPTY_NODE);
      } else {
        int cmp = comparator.compare(key, this.k);
        if (cmp < 0) {
          return node(c, l._put(key, value, merge, comparator), k, v, r).balance();
        } else if (cmp > 0) {
          return node(c, l, k, v, r._put(key, value, merge, comparator)).balance();
        } else {
          return node(c, l, key, merge.apply(v, value), r);
        }
      }
    }

    public void split(int targetSize, IList<Node<K, V>> acc) {
      if (size >= targetSize * 2) {
        l.split(targetSize, acc);
        r.split(targetSize, acc);
      } else if (size > 0) {
        acc.addLast(this);
      }
    }

    public Node<K, V> balance() {
      if (size == 0) {
        return this;
      } else if (c == BLACK) {
        return balanceBlack();
      } else if (c == DOUBLE_BLACK) {
        return balanceDoubleBlack();
      } else {
        return this;
      }
    }

    public Node<K, V> rotate() {
      if (size == 0) {
        return this;
      }

      if (c == RED) {
        // (R (BB? a-x-b) y (B czd))
        // (balance (B (R (-B a-x-b) y c) z d))
        if (l.c == DOUBLE_BLACK && r.c == BLACK) {
          return black(red(l.unblacken(), k, v, r.l), r.k, r.v, r.r).balance();
        }

        // (R (B axb) y (BB? c-z-d))
        // (balance (B a x (R b y (-B c-z-d))))
        if (r.c == DOUBLE_BLACK && l.c == BLACK) {
          return black(l.l, l.k, l.v, red(l.r, k, v, r.unblacken())).balance();
        }
      } else if (c == BLACK) {

        // (B (BB? a-x-b) y (B czd))
        // (balance (BB (R (-B a-x-b) y c) z d))
        if (l.c == DOUBLE_BLACK && r.c == BLACK) {
          return node(DOUBLE_BLACK, red(l.unblacken(), k, v, r.l), r.k, r.v, r.r).balance();
        }

        // (B (B axb) y (BB? c-z-d))
        // (balance (BB a x (R b y (-B c-z-d))))
        if (l.c == BLACK && r.c == DOUBLE_BLACK) {
          return node(DOUBLE_BLACK, l.l, l.k, l.v, red(l.r, k, v, r.unblacken())).balance();
        }

        // (B (BB? a-w-b) x (R (B cyd) z e))
        // (B (balance (B (R (-B a-w-b) x c) y d)) z e)
        if (l.c == DOUBLE_BLACK && r.c == RED && r.l.c == BLACK) {
          Node<K, V> rl = r.l;
          return black(black(red(l.unblacken(), k, v, rl.l), rl.k, rl.v, rl.r).balance(), r.k, r.v, r.r);
        }

        // (B (R a w (B bxc)) y (BB? d-z-e))
        // (B a w (balance (B b x (R c y (-B d-z-e)))))
        if (l.c == RED && l.r.c == BLACK && r.c == DOUBLE_BLACK) {
          Node<K, V> lr = l.r;
          return black(l.l, l.k, l.v, black(lr.l, lr.k, lr.v, red(lr.r, k, v, r.unblacken())).balance());
        }
      }

      return this;
    }

    ///

    private Node<K, V> balanceBlack() {
      if (l.c == RED) {
        // (B (R (R a x b) y c) z d)
        // (R (B a x b) y (B c z d))
        if (l.l.c == RED) {
          return red(l.l.blacken(), l.k, l.v, black(l.r, k, v, r));
        }

        // (B (R a x (R b y c)) z d)
        // (R (B a x b) y (B c z d))
        if (l.r.c == RED) {
          Node<K, V> lr = l.r;
          return red(black(l.l, l.k, l.v, lr.l), lr.k, lr.v, black(lr.r, k, v, r));
        }
      }

      if (r.c == RED) {
        // (B a x (R (R b y c) z d))
        // (R (B a x b) y (B c z d))
        if (r.l.c == RED) {
          Node<K, V> rl = r.l;
          return red(black(l, k, v, rl.l), rl.k, rl.v, black(rl.r, r.k, r.v, r.r));
        }

        // (B a x (R b y (R c z d))
        // (R (B a x b) y (B c z d))
        if (r.r.c == RED) {
          return red(black(l, k, v, r.l), r.k, r.v, r.r.blacken());
        }
      }

      return this;
    }

    private Node<K, V> balanceDoubleBlack() {
      // (BB (R a x (R b y c)) z d)
      // (B (B a x b) y (B c z d))
      if (l.c == RED && l.r.c == RED) {
        Node<K, V> lr = l.r;
        return black(black(l.l, l.k, l.v, lr.l), lr.k, lr.v, black(lr.r, k, v, r));
      }

      // (BB a x (R (R b y c) z d))
      // (B (B a x b) y (B c z d))
      if (r.c == RED && r.l.c == RED) {
        Node<K, V> rl = r.l;
        return black(black(l, k, v, rl.l), rl.k, rl.v, black(rl.r, r.k, r.v, r.r));
      }

      return this;
    }

    private Node<K, V> removeMin() {
      if (l.size == 0) {
        if (c == RED) {
          return EMPTY_NODE;
        } else if (r.size == 0) {
          return DOUBLE_EMPTY_NODE;
        } else {
          return r.blacken();
        }
      }

      return node(c, l.removeMin(), k, v, r).rotate();
    }

    public int checkInvariant() {
      if (c == DOUBLE_BLACK) {
        throw new IllegalStateException();
      }

      if (size == 0) {
        return 1;
      }

      if (c == RED && (l.c == RED || r.c == RED)) {
        throw new IllegalStateException();
      }

      int ld = l.checkInvariant();
      int rd = r.checkInvariant();

      if (ld != rd) {
        throw new IllegalStateException();
      }

      int n = ld;
      if (c == BLACK) {
        n++;
      }

      return n;
    }
  }

  static <K, V> Node<K, V> min(Node<K, V> n) {
    for (; ; ) {
      if (n.l.size == 0) {
        return n;
      } else {
        n = n.l;
      }
    }
  }

  static <K, V> Node<K, V> red(Node<K, V> l, K k, V v, Node<K, V> r) {
    return new Node<>(RED, l, k, v, r);
  }

  static <K, V> Node<K, V> black(Node<K, V> l, K k, V v, Node<K, V> r) {
    return new Node<>(BLACK, l, k, v, r);
  }

  static <K, V> Node<K, V> node(Color c, Node<K, V> l, K k, V v, Node<K, V> r) {
    return new Node<>(c, l, k, v, r);
  }

  public static <K, V> Node<K, V> floor(Node<K, V> n, K key, Comparator<K> comparator) {
    Node<K, V> candidate = null;
    return null;
  }

  public static <K, V> Node<K, V> ceil(Node<K, V> n, K key, Comparator<K> comparator) {
    Node<K, V> candidate = null;
    return null;
  }

  public static <K, V> Node<K, V> slice(Node<K, V> n, K min, K max, Comparator<K> comparator) {
    return null;
  }

  public static <K, V> Node<K, V> find(Node<K, V> n, K key, Comparator<K> comparator) {
    for (; ; ) {
      if (n.size == 0) {
        return null;
      }

      int cmp = comparator.compare(key, n.k);
      if (cmp < 0) {
        n = n.l;
      } else if (cmp > 0) {
        n = n.r;
      } else {
        return n;
      }
    }
  }

  public static <K, V> int indexOf(Node<K, V> n, K key, Comparator<K> comparator) {
    int idx = 0;
    for (; ; ) {
      if (n.size == 0) {
        return -1;
      }

      int cmp = comparator.compare(key, n.k);
      if (cmp < 0) {
        n = n.l;
      } else if (cmp > 0) {
        idx += n.l.size + 1;
        n = n.r;
      } else {
        return idx + n.l.size;
      }
    }
  }

  public static <K, V> Node<K, V> nth(Node<K, V> n, int idx) {
    for (; ; ) {
      if (idx >= n.l.size) {
        idx -= n.l.size + 1;
        if (idx == -1) {
          return n;
        } else {
          n = n.r;
        }
      } else {
        n = n.l;
      }
    }
  }

  public static <K, V> Iterator<IEntry<K, V>> iterator(Node<K, V> root) {

    if (root.size == 0) {
      return Iterators.EMPTY;
    }

    return new Iterator<IEntry<K, V>>() {
      final Node<K, V>[] stack = new Node[64];
      final byte[] cursor = new byte[64];
      int depth = 0;

      {
        stack[0] = root;
        nextValue();
      }

      private void nextValue() {
        while (depth >= 0) {
          Node<K, V> n = stack[depth];
          switch (cursor[depth]) {
            case 0:
              if (n.l.size == 0) {
                cursor[depth]++;
                return;
              } else {
                stack[++depth] = n.l;
                cursor[depth] = 0;
              }
              break;
            case 1:
              return;
            case 2:
              if (n.r.size == 0) {
                if (--depth >= 0) {
                  cursor[depth]++;
                }
              } else {
                stack[++depth] = n.r;
                cursor[depth] = 0;
              }
              break;
            case 3:
              if (--depth >= 0) {
                cursor[depth]++;
              }
          }
        }
      }

      @Override
      public boolean hasNext() {
        return depth >= 0;
      }

      @Override
      public IEntry<K, V> next() {
        Node<K, V> n = stack[depth];
        IEntry<K, V> e = new Maps.Entry<>(n.k, n.v);

        cursor[depth]++;
        nextValue();
        return e;
      }
    };
  }
}
