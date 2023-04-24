package com.github.btrekkie.red_black_node;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A node in a red-black tree ( https://en.wikipedia.org/wiki/Red%E2%80%93black_tree ). Compared to a class like Java's
 * TreeMap, RedBlackNode is a low-level data structure. The internals of a node are exposed as public fields, allowing
 * clients to directly observe and manipulate the structure of the tree. This gives clients flexibility, although it
 * also enables them to violate the red-black or BST properties. The RedBlackNode class provides methods for performing
 * various standard operations, such as insertion and removal.
 *
 * Unlike most implementations of binary search trees, RedBlackNode supports arbitrary augmentation. By subclassing
 * RedBlackNode, clients can add arbitrary data and augmentation information to each node. For example, if we were to
 * use a RedBlackNode subclass to implement a sorted set, the subclass would have a field storing an element in the set.
 * If we wanted to keep track of the number of non-leaf nodes in each subtree, we would store this as a "size" field and
 * override augment() to update this field. All RedBlackNode methods (such as "insert" and remove()) call augment() as
 * necessary to correctly maintain the augmentation information, unless otherwise indicated.
 *
 * The values of the tree are stored in the non-leaf nodes. RedBlackNode does not support use cases where values must be
 * stored in the leaf nodes. It is recommended that all of the leaf nodes in a given tree be the same (black)
 * RedBlackNode instance, to save space. The root of an empty tree is a leaf node, as opposed to null.
 *
 * For reference, a red-black tree is a binary search tree satisfying the following properties:
 *
 * - Every node is colored red or black.
 * - The leaf nodes, which are dummy nodes that do not store any values, are colored black.
 * - The root is black.
 * - Both children of each red node are black.
 * - Every path from the root to a leaf contains the same number of black nodes.
 *
 * @param <N> The type of node in the tree. For example, we might have
 *     "class FooNode<T> extends RedBlackNode<FooNode<T>>".
 * @author Bill Jacobs
 */
public abstract class RedBlackNode<N extends RedBlackNode<N>> implements Comparable<N> {
    /** A Comparator that compares Comparable elements using their natural order. */
    private static final Comparator<Comparable<Object>> NATURAL_ORDER = new Comparator<Comparable<Object>>() {
        @Override
        public int compare(Comparable<Object> value1, Comparable<Object> value2) {
            return value1.compareTo(value2);
        }
    };

    /** The parent of this node, if any.  "parent" is null if this is a leaf node. */
    public N parent;

    /** The left child of this node.  "left" is null if this is a leaf node. */
    public N left;

    /** The right child of this node.  "right" is null if this is a leaf node. */
    public N right;

    /** Whether the node is colored red, as opposed to black. */
    public boolean isRed;

    /**
     * Sets any augmentation information about the subtree rooted at this node that is stored in this node.  For
     * example, if we augment each node by subtree size (the number of non-leaf nodes in the subtree), this method would
     * set the size field of this node to be equal to the size field of the left child plus the size field of the right
     * child plus one.
     *
     * "Augmentation information" is information that we can compute about a subtree rooted at some node, preferably
     * based only on the augmentation information in the node's two children and the information in the node.  Examples
     * of augmentation information are the sum of the values in a subtree and the number of non-leaf nodes in a subtree.
     * Augmentation information may not depend on the colors of the nodes.
     *
     * This method returns whether the augmentation information in any of the ancestors of this node might have been
     * affected by changes in this subtree since the last call to augment().  In the usual case, where the augmentation
     * information depends only on the information in this node and the augmentation information in its immediate
     * children, this is equivalent to whether the augmentation information changed as a result of this call to
     * augment().  For example, in the case of subtree size, this returns whether the value of the size field prior to
     * calling augment() differed from the size field of the left child plus the size field of the right child plus one.
     * False positives are permitted.  The return value is unspecified if we have not called augment() on this node
     * before.
     *
     * This method may assume that this is not a leaf node.  It may not assume that the augmentation information stored
     * in any of the tree's nodes is correct.  However, if the augmentation information stored in all of the node's
     * descendants is correct, then the augmentation information stored in this node must be correct after calling
     * augment().
     */
    public boolean augment() {
        return false;
    }

    /**
     * Throws a RuntimeException if we detect that this node locally violates any invariants specific to this subclass
     * of RedBlackNode.  For example, if this stores the size of the subtree rooted at this node, this should throw a
     * RuntimeException if the size field of this is not equal to the size field of the left child plus the size field
     * of the right child plus one.  Note that we may call this on a leaf node.
     *
     * assertSubtreeIsValid() calls assertNodeIsValid() on each node, or at least starts to do so until it detects a
     * problem.  assertNodeIsValid() should assume the node is in a tree that satisfies all properties common to all
     * red-black trees, as assertSubtreeIsValid() is responsible for such checks.  assertNodeIsValid() should be
     * "downward-looking", i.e. it should ignore any information in "parent", and it should be "local", i.e. it should
     * only check a constant number of descendants.  To include "global" checks, such as verifying the BST property
     * concerning ordering, override assertSubtreeIsValid().  assertOrderIsValid is useful for checking the BST
     * property.
     */
    public void assertNodeIsValid() {

    }

    /** Returns whether this is a leaf node. */
    public boolean isLeaf() {
        return left == null;
    }

    /** Returns the root of the tree that contains this node. */
    public N root() {
        @SuppressWarnings("unchecked")
        N node = (N)this;
        while (node.parent != null) {
            node = node.parent;
        }
        return node;
    }

    /** Returns the first node in the subtree rooted at this node, if any. */
    public N min() {
        if (isLeaf()) {
            return null;
        }
        @SuppressWarnings("unchecked")
        N node = (N)this;
        while (!node.left.isLeaf()) {
            node = node.left;
        }
        return node;
    }

    /** Returns the last node in the subtree rooted at this node, if any. */
    public N max() {
        if (isLeaf()) {
            return null;
        }
        @SuppressWarnings("unchecked")
        N node = (N)this;
        while (!node.right.isLeaf()) {
            node = node.right;
        }
        return node;
    }

    /** Returns the node immediately before this in the tree that contains this node, if any. */
    public N predecessor() {
        if (!left.isLeaf()) {
            N node;
            for (node = left; !node.right.isLeaf(); node = node.right);
            return node;
        } else if (parent == null) {
            return null;
        } else {
            @SuppressWarnings("unchecked")
            N node = (N)this;
            while (node.parent != null && node.parent.left == node) {
                node = node.parent;
            }
            return node.parent;
        }
    }

    /** Returns the node immediately after this in the tree that contains this node, if any. */
    public N successor() {
        if (!right.isLeaf()) {
            N node;
            for (node = right; !node.left.isLeaf(); node = node.left);
            return node;
        } else if (parent == null) {
            return null;
        } else {
            @SuppressWarnings("unchecked")
            N node = (N)this;
            while (node.parent != null && node.parent.right == node) {
                node = node.parent;
            }
            return node.parent;
        }
    }

    /**
     * Performs a left rotation about this node. This method assumes that !isLeaf() && !right.isLeaf(). It calls
     * augment() on this node and on its resulting parent. However, it does not call augment() on any of the resulting
     * parent's ancestors, because that is normally the responsibility of the caller.
     * @return The return value from calling augment() on the resulting parent.
     */
    public boolean rotateLeft() {
        if (isLeaf() || right.isLeaf()) {
            throw new IllegalArgumentException("The node or its right child is a leaf");
        }
        N newParent = right;
        right = newParent.left;
        @SuppressWarnings("unchecked")
        N nThis = (N)this;
        if (!right.isLeaf()) {
            right.parent = nThis;
        }
        newParent.parent = parent;
        parent = newParent;
        newParent.left = nThis;
        if (newParent.parent != null) {
            if (newParent.parent.left == this) {
                newParent.parent.left = newParent;
            } else {
                newParent.parent.right = newParent;
            }
        }
        augment();
        return newParent.augment();
    }

    /**
     * Performs a right rotation about this node. This method assumes that !isLeaf() && !left.isLeaf(). It calls
     * augment() on this node and on its resulting parent. However, it does not call augment() on any of the resulting
     * parent's ancestors, because that is normally the responsibility of the caller.
     * @return The return value from calling augment() on the resulting parent.
     */
    public boolean rotateRight() {
        if (isLeaf() || left.isLeaf()) {
            throw new IllegalArgumentException("The node or its left child is a leaf");
        }
        N newParent = left;
        left = newParent.right;
        @SuppressWarnings("unchecked")
        N nThis = (N)this;
        if (!left.isLeaf()) {
            left.parent = nThis;
        }
        newParent.parent = parent;
        parent = newParent;
        newParent.right = nThis;
        if (newParent.parent != null) {
            if (newParent.parent.left == this) {
                newParent.parent.left = newParent;
            } else {
                newParent.parent.right = newParent;
            }
        }
        augment();
        return newParent.augment();
    }

    /**
     * Performs red-black insertion fixup.  To be more precise, this fixes a tree that satisfies all of the requirements
     * of red-black trees, except that this may be a red child of a red node, and if this is the root, the root may be
     * red.  node.isRed must initially be true.  This method assumes that this is not a leaf node.  The method performs
     * any rotations by calling rotateLeft() and rotateRight().  This method is more efficient than fixInsertion if
     * "augment" is false or augment() might return false.
     * @param augment Whether to set the augmentation information for "node" and its ancestors, by calling augment().
     */
    public void fixInsertionWithoutGettingRoot(boolean augment) {
        if (!isRed) {
            throw new IllegalArgumentException("The node must be red");
        }
        boolean changed = augment;
        if (augment) {
            augment();
        }

        RedBlackNode<N> node = this;
        while (node.parent != null && node.parent.isRed) {
            N parent = node.parent;
            N grandparent = parent.parent;
            if (grandparent.left.isRed && grandparent.right.isRed) {
                grandparent.left.isRed = false;
                grandparent.right.isRed = false;
                grandparent.isRed = true;

                if (changed) {
                    changed = parent.augment();
                    if (changed) {
                        changed = grandparent.augment();
                    }
                }
                node = grandparent;
            } else {
                if (parent.left == node) {
                    if (grandparent.right == parent) {
                        parent.rotateRight();
                        node = parent;
                        parent = node.parent;
                    }
                } else if (grandparent.left == parent) {
                    parent.rotateLeft();
                    node = parent;
                    parent = node.parent;
                }

                if (parent.left == node) {
                    boolean grandparentChanged = grandparent.rotateRight();
                    if (augment) {
                        changed = grandparentChanged;
                    }
                } else {
                    boolean grandparentChanged = grandparent.rotateLeft();
                    if (augment) {
                        changed = grandparentChanged;
                    }
                }

                parent.isRed = false;
                grandparent.isRed = true;
                node = parent;
                break;
            }
        }

        if (node.parent == null) {
            node.isRed = false;
        }
        if (changed) {
            for (node = node.parent; node != null; node = node.parent) {
                if (!node.augment()) {
                    break;
                }
            }
        }
    }

    /**
     * Performs red-black insertion fixup.  To be more precise, this fixes a tree that satisfies all of the requirements
     * of red-black trees, except that this may be a red child of a red node, and if this is the root, the root may be
     * red.  node.isRed must initially be true.  This method assumes that this is not a leaf node.  The method performs
     * any rotations by calling rotateLeft() and rotateRight().  This method is more efficient than fixInsertion() if
     * augment() might return false.
     */
    public void fixInsertionWithoutGettingRoot() {
        fixInsertionWithoutGettingRoot(true);
    }

    /**
     * Performs red-black insertion fixup.  To be more precise, this fixes a tree that satisfies all of the requirements
     * of red-black trees, except that this may be a red child of a red node, and if this is the root, the root may be
     * red.  node.isRed must initially be true.  This method assumes that this is not a leaf node.  The method performs
     * any rotations by calling rotateLeft() and rotateRight().
     * @param augment Whether to set the augmentation information for "node" and its ancestors, by calling augment().
     * @return The root of the resulting tree.
     */
    public N fixInsertion(boolean augment) {
        fixInsertionWithoutGettingRoot(augment);
        return root();
    }

    /**
     * Performs red-black insertion fixup.  To be more precise, this fixes a tree that satisfies all of the requirements
     * of red-black trees, except that this may be a red child of a red node, and if this is the root, the root may be
     * red.  node.isRed must initially be true.  This method assumes that this is not a leaf node.  The method performs
     * any rotations by calling rotateLeft() and rotateRight().
     * @return The root of the resulting tree.
     */
    public N fixInsertion() {
        fixInsertionWithoutGettingRoot(true);
        return root();
    }

    /** Returns a Comparator that compares instances of N using their natural order, as in N.compareTo. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Comparator<N> naturalOrder() {
        Comparator comparator = (Comparator)NATURAL_ORDER;
        return (Comparator<N>)comparator;
    }

    /**
     * Inserts the specified node into the tree rooted at this node. Assumes this is the root. We treat newNode as a
     * solitary node that does not belong to any tree, and we ignore its initial "parent", "left", "right", and isRed
     * fields.
     *
     * If it is not efficient or convenient to find the location for a node using a Comparator, then you should manually
     * add the node to the appropriate location, color it red, and call fixInsertion().
     *
     * @param newNode The node to insert.
     * @param allowDuplicates Whether to insert newNode if there is an equal node in the tree. To check whether we
     *     inserted newNode, check whether newNode.parent is null and the return value differs from newNode.
     * @param comparator A comparator indicating where to put the node. If this is null, we use the nodes' natural
     *     order, as in N.compareTo. If you are passing null, then you must override the compareTo method, because the
     *     default implementation requires the nodes to already be in the same tree.
     * @return The root of the resulting tree.
     */
    public N insert(N newNode, boolean allowDuplicates, Comparator<? super N> comparator) {
        if (parent != null) {
            throw new IllegalArgumentException("This is not the root of a tree");
        }
        @SuppressWarnings("unchecked")
        N nThis = (N)this;
        if (isLeaf()) {
            newNode.isRed = false;
            newNode.left = nThis;
            newNode.right = nThis;
            newNode.parent = null;
            newNode.augment();
            return newNode;
        }
        if (comparator == null) {
            comparator = naturalOrder();
        }

        N node = nThis;
        int comparison;
        while (true) {
            comparison = comparator.compare(newNode, node);
            if (comparison < 0) {
                if (!node.left.isLeaf()) {
                    node = node.left;
                } else {
                    newNode.left = node.left;
                    newNode.right = node.left;
                    node.left = newNode;
                    newNode.parent = node;
                    break;
                }
            } else if (comparison > 0 || allowDuplicates) {
                if (!node.right.isLeaf()) {
                    node = node.right;
                } else {
                    newNode.left = node.right;
                    newNode.right = node.right;
                    node.right = newNode;
                    newNode.parent = node;
                    break;
                }
            } else {
                newNode.parent = null;
                return nThis;
            }
        }
        newNode.isRed = true;
        return newNode.fixInsertion();
    }

    /**
     * Moves this node to its successor's former position in the tree and vice versa, i.e. sets the "left", "right",
     * "parent", and isRed fields of each.  This method assumes that this is not a leaf node.
     * @return The node with which we swapped.
     */
    private N swapWithSuccessor() {
        N replacement = successor();
        boolean oldReplacementIsRed = replacement.isRed;
        N oldReplacementLeft = replacement.left;
        N oldReplacementRight = replacement.right;
        N oldReplacementParent = replacement.parent;

        replacement.isRed = isRed;
        replacement.left = left;
        replacement.right = right;
        replacement.parent = parent;
        if (parent != null) {
            if (parent.left == this) {
                parent.left = replacement;
            } else {
                parent.right = replacement;
            }
        }

        @SuppressWarnings("unchecked")
        N nThis = (N)this;
        isRed = oldReplacementIsRed;
        left = oldReplacementLeft;
        right = oldReplacementRight;
        if (oldReplacementParent == this) {
            parent = replacement;
            parent.right = nThis;
        } else {
            parent = oldReplacementParent;
            parent.left = nThis;
        }

        replacement.right.parent = replacement;
        if (!replacement.left.isLeaf()) {
            replacement.left.parent = replacement;
        }
        if (!right.isLeaf()) {
            right.parent = nThis;
        }
        return replacement;
    }

    /**
     * Performs red-black deletion fixup.  To be more precise, this fixes a tree that satisfies all of the requirements
     * of red-black trees, except that all paths from the root to a leaf that pass through the sibling of this node have
     * one fewer black node than all other root-to-leaf paths.  This method assumes that this is not a leaf node.
     */
    private void fixSiblingDeletion() {
        RedBlackNode<N> sibling = this;
        boolean changed = true;
        boolean haveAugmentedParent = false;
        boolean haveAugmentedGrandparent = false;
        while (true) {
            N parent = sibling.parent;
            if (sibling.isRed) {
                parent.isRed = true;
                sibling.isRed = false;
                if (parent.left == sibling) {
                    changed = parent.rotateRight();
                    sibling = parent.left;
                } else {
                    changed = parent.rotateLeft();
                    sibling = parent.right;
                }
                haveAugmentedParent = true;
                haveAugmentedGrandparent = true;
            } else if (!sibling.left.isRed && !sibling.right.isRed) {
                sibling.isRed = true;
                if (parent.isRed) {
                    parent.isRed = false;
                    break;
                } else {
                    if (changed && !haveAugmentedParent) {
                        changed = parent.augment();
                    }
                    N grandparent = parent.parent;
                    if (grandparent == null) {
                        break;
                    } else if (grandparent.left == parent) {
                        sibling = grandparent.right;
                    } else {
                        sibling = grandparent.left;
                    }
                    haveAugmentedParent = haveAugmentedGrandparent;
                    haveAugmentedGrandparent = false;
                }
            } else {
                if (sibling == parent.left) {
                    if (!sibling.left.isRed) {
                        sibling.rotateLeft();
                        sibling = sibling.parent;
                    }
                } else if (!sibling.right.isRed) {
                    sibling.rotateRight();
                    sibling = sibling.parent;
                }
                sibling.isRed = parent.isRed;
                parent.isRed = false;
                if (sibling == parent.left) {
                    sibling.left.isRed = false;
                    changed = parent.rotateRight();
                } else {
                    sibling.right.isRed = false;
                    changed = parent.rotateLeft();
                }
                haveAugmentedParent = haveAugmentedGrandparent;
                haveAugmentedGrandparent = false;
                break;
            }
        }

        // Update augmentation info
        N parent = sibling.parent;
        if (changed && parent != null) {
            if (!haveAugmentedParent) {
                changed = parent.augment();
            }
            if (changed && parent.parent != null) {
                parent = parent.parent;
                if (!haveAugmentedGrandparent) {
                    changed = parent.augment();
                }
                if (changed) {
                    for (parent = parent.parent; parent != null; parent = parent.parent) {
                        if (!parent.augment()) {
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Removes this node from the tree that contains it.  The effect of this method on the fields of this node is
     * unspecified.  This method assumes that this is not a leaf node.  This method is more efficient than remove() if
     * augment() might return false.
     *
     * If the node has two children, we begin by moving the node's successor to its former position, by changing the
     * successor's "left", "right", "parent", and isRed fields.
     */
    public void removeWithoutGettingRoot() {
        if (isLeaf()) {
            throw new IllegalArgumentException("Attempted to remove a leaf node");
        }
        N replacement;
        if (left.isLeaf() || right.isLeaf()) {
            replacement = null;
        } else {
            replacement = swapWithSuccessor();
        }

        N child;
        if (!left.isLeaf()) {
            child = left;
        } else if (!right.isLeaf()) {
            child = right;
        } else {
            child = null;
        }

        if (child != null) {
            // Replace this node with its child
            child.parent = parent;
            if (parent != null) {
                if (parent.left == this) {
                    parent.left = child;
                } else {
                    parent.right = child;
                }
            }
            child.isRed = false;

            if (child.parent != null) {
                N parent;
                for (parent = child.parent; parent != null; parent = parent.parent) {
                    if (!parent.augment()) {
                        break;
                    }
                }
            }
        } else if (parent != null) {
            // Replace this node with a leaf node
            N leaf = left;
            N parent = this.parent;
            N sibling;
            if (parent.left == this) {
                parent.left = leaf;
                sibling = parent.right;
            } else {
                parent.right = leaf;
                sibling = parent.left;
            }

            if (!isRed) {
                RedBlackNode<N> siblingNode = sibling;
                siblingNode.fixSiblingDeletion();
            } else {
                while (parent != null) {
                    if (!parent.augment()) {
                        break;
                    }
                    parent = parent.parent;
                }
            }
        }

        if (replacement != null) {
            replacement.augment();
            for (N parent = replacement.parent; parent != null; parent = parent.parent) {
                if (!parent.augment()) {
                    break;
                }
            }
        }

        // Clear any previously existing links, so that we're more likely to encounter an exception if we attempt to
        // access the removed node
        parent = null;
        left = null;
        right = null;
        isRed = true;
    }

    /**
     * Removes this node from the tree that contains it.  The effect of this method on the fields of this node is
     * unspecified.  This method assumes that this is not a leaf node.
     *
     * If the node has two children, we begin by moving the node's successor to its former position, by changing the
     * successor's "left", "right", "parent", and isRed fields.
     *
     * @return The root of the resulting tree.
     */
    public N remove() {
        if (isLeaf()) {
            throw new IllegalArgumentException("Attempted to remove a leaf node");
        }

        // Find an arbitrary non-leaf node in the tree other than this node
        N node;
        if (parent != null) {
            node = parent;
        } else if (!left.isLeaf()) {
            node = left;
        } else if (!right.isLeaf()) {
            node = right;
        } else {
            return left;
        }

        removeWithoutGettingRoot();
        return node.root();
    }

    /**
     * Returns the root of a perfectly height-balanced subtree containing the next "size" (non-leaf) nodes from
     * "iterator", in iteration order.  This method is responsible for setting the "left", "right", "parent", and isRed
     * fields of the nodes, and calling augment() as appropriate.  It ignores the initial values of the "left", "right",
     * "parent", and isRed fields.
     * @param iterator The nodes.
     * @param size The number of nodes.
     * @param height The "height" of the subtree's root node above the deepest leaf in the tree that contains it.  Since
     *     insertion fixup is slow if there are too many red nodes and deleteion fixup is slow if there are too few red
     *     nodes, we compromise and have red nodes at every fourth level.  We color a node red iff its "height" is equal
     *     to 1 mod 4.
     * @param leaf The leaf node.
     * @return The root of the subtree.
     */
    private static <N extends RedBlackNode<N>> N createTree(
            Iterator<? extends N> iterator, int size, int height, N leaf) {
        if (size == 0) {
            return leaf;
        } else {
            N left = createTree(iterator, (size - 1) / 2, height - 1, leaf);
            N node = iterator.next();
            N right = createTree(iterator, size / 2, height - 1, leaf);

            node.isRed = height % 4 == 1;
            node.left = left;
            node.right = right;
            if (!left.isLeaf()) {
                left.parent = node;
            }
            if (!right.isLeaf()) {
                right.parent = node;
            }

            node.augment();
            return node;
        }
    }

    /**
     * Returns the root of a perfectly height-balanced tree containing the specified nodes, in iteration order. This
     * method is responsible for setting the "left", "right", "parent", and isRed fields of the nodes (excluding
     * "leaf"), and calling augment() as appropriate. It ignores the initial values of the "left", "right", "parent",
     * and isRed fields.
     * @param nodes The nodes.
     * @param leaf The leaf node.
     * @return The root of the tree.
     */
    public static <N extends RedBlackNode<N>> N createTree(Collection<? extends N> nodes, N leaf) {
        int size = nodes.size();
        if (size == 0) {
            return leaf;
        }

        int height = 0;
        for (int subtreeSize = size; subtreeSize > 0; subtreeSize /= 2) {
            height++;
        }

        N node = createTree(nodes.iterator(), size, height, leaf);
        node.parent = null;
        node.isRed = false;
        return node;
    }

    /**
     * Implementation of concatenate(last, pivot) for when blackHeight() and last.blackHeight() are known. These are
     * given by firstBlackHeight and lastBlackHeight respectively.
     */
    private N concatenate(N last, N pivot, int firstBlackHeight, int lastBlackHeight) {
        // If the black height of "first", where first = this, is less than or equal to that of "last", starting at the
        // root of "last", we keep going left until we reach a black node whose black height is equal to that of
        // "first".  Then, we make "pivot" the parent of that node and of "first", coloring it red, and perform
        // insertion fixup on the pivot.  If the black height of "first" is greater than that of "last", we do the
        // mirror image of the above.

        // Identify the children and parent of pivot
        @SuppressWarnings("unchecked")
        N firstChild = (N)this;
        N lastChild = last;
        N parent;
        if (firstBlackHeight <= lastBlackHeight) {
            parent = null;
            int blackHeight = lastBlackHeight;
            while (blackHeight > firstBlackHeight) {
                if (!lastChild.isRed) {
                    blackHeight--;
                }
                parent = lastChild;
                lastChild = lastChild.left;
            }
            if (lastChild.isRed) {
                parent = lastChild;
                lastChild = lastChild.left;
            }
        } else {
            parent = null;
            int blackHeight = firstBlackHeight;
            while (blackHeight > lastBlackHeight) {
                if (!firstChild.isRed) {
                    blackHeight--;
                }
                parent = firstChild;
                firstChild = firstChild.right;
            }
            if (firstChild.isRed) {
                parent = firstChild;
                firstChild = firstChild.right;
            }
        }

        // Add "pivot" to the tree
        pivot.isRed = true;
        pivot.parent = parent;
        if (parent != null) {
            if (firstBlackHeight < lastBlackHeight) {
                parent.left = pivot;
            } else {
                parent.right = pivot;
            }
        }
        pivot.left = firstChild;
        if (!firstChild.isLeaf()) {
            firstChild.parent = pivot;
        }
        pivot.right = lastChild;
        if (!lastChild.isLeaf()) {
            lastChild.parent = pivot;
        }

        // Perform insertion fixup
        return pivot.fixInsertion();
    }

    /** Returns the number of black nodes in a path from this to a leaf node (including this and the leaf node). */
    private int blackHeight() {
        int blackHeight = 0;
        for (RedBlackNode<N> node = this; node != null; node = node.right) {
            if (!node.isRed) {
                blackHeight++;
            }
        }
        return blackHeight;
    }

    /**
     * Concatenates to the end of the tree rooted at this node.  To be precise, given that all of the nodes in this
     * precede the node "pivot", which precedes all of the nodes in "last", this returns the root of a tree containing
     * all of these nodes.  This method destroys the trees rooted at "this" and "last".  We treat "pivot" as a solitary
     * node that does not belong to any tree, and we ignore its initial "parent", "left", "right", and isRed fields.
     * This method assumes that this node and "last" are the roots of their respective trees.
     *
     * This method takes O(log N) time.  It is more efficient than inserting "pivot" and then calling concatenate(last).
     * It is considerably more efficient than inserting "pivot" and all of the nodes in "last".
     */
    public N concatenate(N last, N pivot) {
        if (parent != null || last.parent != null) {
            throw new IllegalArgumentException("The node is not the root of a tree");
        }
        return concatenate(last, pivot, blackHeight(), ((RedBlackNode<N>)last).blackHeight());
    }

    /**
     * Concatenates the tree rooted at "last" to the end of the tree rooted at this node.  To be precise, given that all
     * of the nodes in this precede all of the nodes in "last", this returns the root of a tree containing all of these
     * nodes.  This method destroys the trees rooted at "this" and "last".  It assumes that this node and "last" are the
     * roots of their respective trees.  This method takes O(log N) time.  It is considerably more efficient than
     * inserting all of the nodes in "last".
     */
    public N concatenate(N last) {
        if (parent != null || last.parent != null) {
            throw new IllegalArgumentException("The node is not the root of a tree");
        }
        if (isLeaf()) {
            return last;
        } else if (last.isLeaf()) {
            @SuppressWarnings("unchecked")
            N nThis = (N)this;
            return nThis;
        } else {
            N node = last.min();
            last = node.remove();
            return concatenate(last, node);
        }
    }

    /**
     * Splits the tree rooted at this node into two trees, so that the first element of the return value is the root of
     * a tree consisting of the nodes that were before the specified node, and the second element of the return value is
     * the root of a tree consisting of the nodes that were equal to or after the specified node. This method is
     * destructive, meaning it does not preserve the original tree. It assumes that this node is the root and is in the
     * same tree as splitNode. It takes O(log N) time. It is considerably more efficient than removing all of the
     * nodes at or after splitNode and then creating a new tree from those nodes.
     * @param splitNode The node at which to split the tree.
     * @return An array consisting of the resulting trees.
     */
    public N[] split(N splitNode) {
        // To split the tree, we accumulate a pre-split tree rooted at firstRoot and a post-split tree rooted at
        // lastRoot. After initializing the trees, we walk up from splitNode or one of its ancestors to the root of the
        // tree. Whenever we go up and to the left, we concatenate the current node's left child with the pre-split
        // tree, using the current node as the pivot. Whenever we go up and to the right, we concatenate the post-split
        // tree with the current node's right child, using the current node as the pivot. We maintain
        // firstRoot.blackHeight() and lastRoot.blackHeight() using the firstBlackHeight and lastBlackHeight variables.
        if (parent != null) {
            throw new IllegalArgumentException("This is not the root of a tree");
        }
        if (isLeaf() || splitNode.isLeaf()) {
            throw new IllegalArgumentException("The root or the split node is a leaf");
        }
        if (splitNode.root() != this) {
            throw new IllegalArgumentException("The split node does not belong to this tree");
        }

        N pivot;
        boolean isConcatenateLeft;
        int concatenateBlackHeight;
        N firstRoot;
        int firstBlackHeight;
        N lastRoot;
        int lastBlackHeight;
        if (!splitNode.left.isLeaf()) {
            pivot = splitNode;
            isConcatenateLeft = false;
            concatenateBlackHeight = ((RedBlackNode<N>)splitNode.right).blackHeight();

            // Initialize the pre-split tree
            firstRoot = splitNode.left;
            firstBlackHeight = concatenateBlackHeight;
            firstRoot.parent = null;
            if (firstRoot.isRed) {
                firstRoot.isRed = false;
                firstBlackHeight++;
            }

            lastRoot = splitNode.max().right;
            lastBlackHeight = 1;
        } else {
            // Note that this branch is not needed for correctness, but it improves performance
            N node;
            for (node = splitNode; node.parent != null && node.parent.left == node; node = node.parent);
            pivot = node.parent;
            isConcatenateLeft = true;
            concatenateBlackHeight = ((RedBlackNode<N>)node).blackHeight();

            // Initialize the post-split tree
            lastRoot = node;
            lastBlackHeight = concatenateBlackHeight;
            lastRoot.parent = null;
            if (lastRoot.isRed) {
                lastRoot.isRed = false;
                lastBlackHeight++;
            }

            firstRoot = splitNode.left;
            firstBlackHeight = 1;
        }

        while (pivot != null) {
            // At this point, it is invariant that:
            //
            // firstBlackHeight <= concatenateBlackHeight ||
            //     (firstBlackHeight == concatenateBlackHeight + 1 &&
            //         !pivot.isRed && !firstRoot.left.isRed && !firstRoot.right.isRed)
            //
            // Likewise for lastRoot and lastBlackHeight.
            N nextPivot = pivot.parent;
            boolean nextIsConcatenateLeft = nextPivot != null && nextPivot.right == pivot;
            int nextConcatenateBlackHeight = concatenateBlackHeight + (pivot.isRed ? 0 : 1);

            N concatenateRoot = isConcatenateLeft ? pivot.left : pivot.right;
            if (isConcatenateLeft && firstBlackHeight >= concatenateBlackHeight && !pivot.isRed) {
                // This branch isn't strictly necessary for correctness or for O(log N) running time. However, it
                // improves performance because it potentially saves us from calling augment() on firstRoot twice.
                if (firstBlackHeight > concatenateBlackHeight) {
                    // The children of firstRoot are black per the loop invariant
                    firstRoot.isRed = true;
                }
                pivot.parent = null;
                pivot.right = firstRoot;
                // Already true: pivot.left == concatenateRoot, concatenateRoot.parent == pivot, !pivot.isRed
                if (!firstRoot.isLeaf()) {
                    firstRoot.parent = pivot;
                }
                firstRoot = pivot;
                firstRoot.augment();
                firstBlackHeight = concatenateBlackHeight + 1;
            } else if (!isConcatenateLeft && lastBlackHeight >= concatenateBlackHeight && !pivot.isRed) {
                // As above, this branch is unnecessary, but saves us from calling augment() on lastRoot twice
                if (lastBlackHeight > concatenateBlackHeight) {
                    // The children of lastRoot are black per the loop invariant
                    lastRoot.isRed = true;
                }
                pivot.parent = null;
                pivot.left = lastRoot;
                // Already true: pivot.right == concatenateRoot, concatenateRoot.parent == pivot, !pivot.isRed
                if (!lastRoot.isLeaf()) {
                    lastRoot.parent = pivot;
                }
                lastRoot = pivot;
                lastRoot.augment();
                lastBlackHeight = concatenateBlackHeight + 1;
            } else {
                concatenateRoot.parent = null;
                if (concatenateRoot.isRed) {
                    concatenateRoot.isRed = false;
                    concatenateBlackHeight++;
                }
                boolean wereChildrenRed =
                    !concatenateRoot.isLeaf() && concatenateRoot.left.isRed && concatenateRoot.right.isRed;

                if (isConcatenateLeft) {
                    firstRoot = ((RedBlackNode<N>)concatenateRoot).concatenate(
                        firstRoot, pivot, concatenateBlackHeight, firstBlackHeight);
                    if (firstBlackHeight >= concatenateBlackHeight || (wereChildrenRed && !firstRoot.left.isRed)) {
                        firstBlackHeight = concatenateBlackHeight + 1;
                    } else {
                        firstBlackHeight = concatenateBlackHeight;
                    }
                } else {
                    lastRoot = ((RedBlackNode<N>)lastRoot).concatenate(
                        concatenateRoot, pivot, lastBlackHeight, concatenateBlackHeight);
                    if (lastBlackHeight >= concatenateBlackHeight || (wereChildrenRed && !lastRoot.left.isRed)) {
                        lastBlackHeight = concatenateBlackHeight + 1;
                    } else {
                        lastBlackHeight = concatenateBlackHeight;
                    }
                }
            }

            pivot = nextPivot;
            isConcatenateLeft = nextIsConcatenateLeft;
            concatenateBlackHeight = nextConcatenateBlackHeight;
        }

        @SuppressWarnings("unchecked")
        N[] result = (N[])Array.newInstance(getClass(), 2);
        result[0] = firstRoot;
        result[1] = lastRoot;
        return result;
    }

    /**
     * Returns the depth of this node minus the depth of "other". Throws an IllegalArgumentException instead if the
     * nodes do not belong to the same tree or one of them is a leaf node. This method takes O(P) time, where P is the
     * length of the path from this node to "other".
     */
    private int depthDiff(N other) {
        if (isLeaf() || other.isLeaf()) {
            throw new IllegalArgumentException("One of the nodes is a leaf node");
        }

        int distance = 0;
        RedBlackNode<N> parent = this;
        int otherDistance = 0;
        N otherParent = other;
        while (parent.parent != null && otherParent.parent != null) {
            do {
                parent = parent.parent;
                distance++;
                if (parent == otherParent) {
                    return distance - otherDistance;
                }
                if (parent.parent == null) {
                    break;
                }
            } while (distance < 2 * otherDistance);

            while (otherDistance < 2 * distance) {
                otherParent = otherParent.parent;
                otherDistance++;
                if (otherParent == parent) {
                    return distance - otherDistance;
                }
                if (otherParent.parent == null) {
                    break;
                }
            }
        }

        while (parent.parent != null) {
            parent = parent.parent;
            distance++;
        }
        while (otherParent.parent != null) {
            otherParent = otherParent.parent;
            otherDistance++;
        }
        if (parent != otherParent) {
            throw new IllegalArgumentException("The nodes do not belong to the same tree");
        }
        return distance - otherDistance;
    }

    /**
     * Returns the lowest common ancestor of this node and "other" - the node that is an ancestor of both and is not the
     * parent of a node that is an ancestor of both. Assumes that this is in the same tree as "other". Assumes that
     * neither "this" nor "other" is a leaf node. This method may return "this" or "other".
     *
     * This method takes O(log N) time, or more precisely, O(P) time, where P is the length of the path from this node
     * to "other".
     */
    public N lca(N other) {
        // Go up to nodes of the same depth
        int depthDiff = depthDiff(other);
        RedBlackNode<N> parent = this;
        N otherParent = other;
        if (depthDiff >= 0) {
            for (int i = 0; i < depthDiff; i++) {
                parent = parent.parent;
            }
        } else {
            for (int i = 0; i < -depthDiff; i++) {
                otherParent = otherParent.parent;
            }
        }

        // Find the LCA
        while (parent != otherParent) {
            parent = parent.parent;
            otherParent = otherParent.parent;
        }
        return otherParent;
    }

    /**
     * Returns an integer comparing the position of this node in the tree that contains it with that of "other". Returns
     * a negative number if this is earlier, a positive number if this is later, and 0 if this is at the same position.
     * Assumes that this is in the same tree as "other". Assumes that neither "this" nor "other" is a leaf node.
     *
     * The base class's implementation takes O(log N) time, or more precisely, O(P) time, where P is the length of the
     * path from this node to "other". If a RedBlackNode subclass stores a value used to order the nodes, then it could
     * override compareTo to compare the nodes' values, which would take O(1) time.
     */
    @Override
    public int compareTo(N other) {
        // The algorithm operates as follows: compare the depth of this node to that of "other".  If the depth of
        // "other" is greater, keep moving up from "other" until we find the ancestor at the same depth.  Then, keep
        // moving up from "this" and from that node until we reach the lowest common ancestor.  The node that arrived
        // from the left child of the common ancestor is earlier.  The algorithm is analogous if the depth of "other" is
        // not greater.
        if (this == other) {
            return 0;
        }

        // Go up to nodes of the same depth
        int depthDiff = depthDiff(other);
        RedBlackNode<N> parent = this;
        N otherParent = other;
        if (depthDiff > 0) {
            for (int i = 0; i + 1 < depthDiff; i++) {
                parent = parent.parent;
            }
            if (parent.parent != other) {
                parent = parent.parent;
            } else if (other.left == parent) {
                return -1;
            } else {
                return 1;
            }
        } else if (depthDiff < 0) {
            for (int i = 0; i + 1 < -depthDiff; i++) {
                otherParent = otherParent.parent;
            }
            if (otherParent.parent != this) {
                otherParent = otherParent.parent;
            } else if (left == otherParent) {
                return 1;
            } else {
                return -1;
            }
        }

        // Keep going up until we reach the lowest common ancestor
        while (parent.parent != otherParent.parent) {
            parent = parent.parent;
            otherParent = otherParent.parent;
        }
        if (parent.parent.left == parent) {
            return -1;
        } else {
            return 1;
        }
    }

    /** Throws a RuntimeException if the RedBlackNode fields of this are not correct for a leaf node. */
    private void assertIsValidLeaf() {
        if (left != null || right != null || parent != null || isRed) {
            throw new RuntimeException("A leaf node's \"left\", \"right\", \"parent\", or isRed field is incorrect");
        }
    }

    /**
     * Throws a RuntimeException if the subtree rooted at this node does not satisfy the red-black properties, excluding
     * the requirement that the root be black, or it contains a repeated node other than a leaf node.
     * @param blackHeight The required number of black nodes in each path from this to a leaf node, including this and
     *     the leaf node.
     * @param visited The nodes we have reached thus far, other than leaf nodes. This method adds the non-leaf nodes in
     *     the subtree rooted at this node to "visited".
     */
    private void assertSubtreeIsValidRedBlack(int blackHeight, Set<Reference<N>> visited) {
        @SuppressWarnings("unchecked")
        N nThis = (N)this;
        if (left == null || right == null) {
            assertIsValidLeaf();
            if (blackHeight != 1) {
                throw new RuntimeException("Not all root-to-leaf paths have the same number of black nodes");
            }
            return;
        } else if (!visited.add(new Reference<N>(nThis))) {
            throw new RuntimeException("The tree contains a repeated non-leaf node");
        } else {
            int childBlackHeight;
            if (isRed) {
                if ((!left.isLeaf() && left.isRed) || (!right.isLeaf() && right.isRed)) {
                    throw new RuntimeException("A red node has a red child");
                }
                childBlackHeight = blackHeight;
            } else if (blackHeight == 0) {
                throw new RuntimeException("Not all root-to-leaf paths have the same number of black nodes");
            } else {
                childBlackHeight = blackHeight - 1;
            }

            if (!left.isLeaf() && left.parent != this) {
                throw new RuntimeException("left.parent != this");
            }
            if (!right.isLeaf() && right.parent != this) {
                throw new RuntimeException("right.parent != this");
            }
            RedBlackNode<N> leftNode = left;
            RedBlackNode<N> rightNode = right;
            leftNode.assertSubtreeIsValidRedBlack(childBlackHeight, visited);
            rightNode.assertSubtreeIsValidRedBlack(childBlackHeight, visited);
        }
    }

    /** Calls assertNodeIsValid() on every node in the subtree rooted at this node. */
    private void assertNodesAreValid() {
        assertNodeIsValid();
        if (left != null) {
            RedBlackNode<N> leftNode = left;
            RedBlackNode<N> rightNode = right;
            leftNode.assertNodesAreValid();
            rightNode.assertNodesAreValid();
        }
    }

    /**
     * Throws a RuntimeException if the subtree rooted at this node is not a valid red-black tree, e.g. if a red node
     * has a red child or it contains a non-leaf node "node" for which node.left.parent != node. (If parent != null,
     * it's okay if isRed is true.) This method is useful for debugging. See also assertSubtreeIsValid().
     */
    public void assertSubtreeIsValidRedBlack() {
        if (isLeaf()) {
            assertIsValidLeaf();
        } else {
            if (parent == null && isRed) {
                throw new RuntimeException("The root is red");
            }

            // Compute the black height of the tree
            Set<Reference<N>> nodes = new HashSet<Reference<N>>();
            int blackHeight = 0;
            @SuppressWarnings("unchecked")
            N node = (N)this;
            while (node != null) {
                if (!nodes.add(new Reference<N>(node))) {
                    throw new RuntimeException("The tree contains a repeated non-leaf node");
                }
                if (!node.isRed) {
                    blackHeight++;
                }
                node = node.left;
            }

            assertSubtreeIsValidRedBlack(blackHeight, new HashSet<Reference<N>>());
        }
    }

    /**
     * Throws a RuntimeException if we detect a problem with the subtree rooted at this node, such as a red child of a
     * red node or a non-leaf descendant "node" for which node.left.parent != node.  This method is useful for
     * debugging.  RedBlackNode subclasses may want to override assertSubtreeIsValid() to call assertOrderIsValid.
     */
    public void assertSubtreeIsValid() {
        assertSubtreeIsValidRedBlack();
        assertNodesAreValid();
    }

    /**
     * Throws a RuntimeException if the nodes in the subtree rooted at this node are not in the specified order or they
     * do not lie in the specified range.  Assumes that the subtree rooted at this node is a valid binary tree, i.e. it
     * has no repeated nodes other than leaf nodes.
     * @param comparator A comparator indicating how the nodes should be ordered.
     * @param start The lower limit for nodes in the subtree, if any.
     * @param end The upper limit for nodes in the subtree, if any.
     */
    private void assertOrderIsValid(Comparator<? super N> comparator, N start, N end) {
        if (!isLeaf()) {
            @SuppressWarnings("unchecked")
            N nThis = (N)this;
            if (start != null && comparator.compare(nThis, start) < 0) {
                throw new RuntimeException("The nodes are not ordered correctly");
            }
            if (end != null && comparator.compare(nThis, end) > 0) {
                throw new RuntimeException("The nodes are not ordered correctly");
            }
            RedBlackNode<N> leftNode = left;
            RedBlackNode<N> rightNode = right;
            leftNode.assertOrderIsValid(comparator, start, nThis);
            rightNode.assertOrderIsValid(comparator, nThis, end);
        }
    }

    /**
     * Throws a RuntimeException if the nodes in the subtree rooted at this node are not in the specified order.
     * Assumes that this is a valid binary tree, i.e. there are no repeated nodes other than leaf nodes.  This method is
     * useful for debugging.  RedBlackNode subclasses may want to override assertSubtreeIsValid() to call
     * assertOrderIsValid.
     * @param comparator A comparator indicating how the nodes should be ordered.  If this is null, we use the nodes'
     *     natural order, as in N.compareTo.
     */
    public void assertOrderIsValid(Comparator<? super N> comparator) {
        if (comparator == null) {
            comparator = naturalOrder();
        }
        assertOrderIsValid(comparator, null, null);
    }
}
