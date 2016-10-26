package io.lacuna.bifurcan.nodes;

import static java.lang.Integer.bitCount;

/**
 * @author ztellman
 */
public class ChampNode {
    int datamap = 0;
    int nodemap = 0;
    int[] hashes;
    Object[] content;

    static int compressedIndex(int bitmap, int hashMask) {
        return bitCount(bitmap & (hashMask - 1));
    }

    static int hashMask(int hash, int shift) {
        return 1 << ((hash >>> shift) & 31);
    }

    int entryIndex(int hash, int shift, int widthShift) {
        return compressedIndex(datamap, hashMask(hash, shift)) << widthShift;
    }

    int nodeIndex(int hash, int shift) {
        return content.length - 1 - compressedIndex(nodemap, hashMask(hash, shift));
    }

    boolean isEntry(int hashMask) {
        return (datamap & hashMask) != 0;
    }

    boolean isNode(int hashMask) {
        return (nodemap & hashMask) != 0;
    }




}
