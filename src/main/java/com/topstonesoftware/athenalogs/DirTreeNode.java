package com.topstonesoftware.athenalogs;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * <p>
 * A node in an S3 "directory" tree (e.g., a tree formed by S3 path keys).
 * </p>
 * <p>
 *     Each node has a directory name and a set of zero or more children.
 * </p>
 */
public class DirTreeNode {
    @Getter
    private final String dirName;
    private final Map<String, DirTreeNode> childMap = new HashMap<>();
    @Getter
    @Setter
    private Integer pageRefCnt = null;

    public DirTreeNode(String name) {
        this.dirName = name;
    }

    public DirTreeNode addChild(String name) {
        return childMap.computeIfAbsent(name, DirTreeNode::new);
    }


    /**
     * Add a path as a sub-tree to this node.  Example of a path string:
     * <pre>
     *     misl/misl_tech/signal/idft/index.html
     * </pre>
     *
     * @param pathInfo a pair containing an S3 path key and a reference count.
     */
    public void addPath(Pair<String, Integer> pathInfo) {
        String path = pathInfo.getLeft();
        String[] pathNames = path.split("/");
        DirTreeNode root = this;
        for (int i = 0; i < pathNames.length; i++) {
            root = root.addChild(pathNames[i]);
            if (i == pathNames.length-1) {
                root.setPageRefCnt( pathInfo.getRight() );
            }
        }
    }

    /**
     * <p>
     *     On the values() function, from the Java documentation:
     * </p>
     * <blockquote>
     *     Returns a Collection view of the values contained in this map. The collection is backed by the map, so
     *     changes to the map are reflected in the collection, and vice-versa.
     * </blockquote>
     * @return a list of the DirTreeNode children for this node.
     */
    public List<DirTreeNode> getChildren() {
        return new ArrayList<>(childMap.values());
    }

}
