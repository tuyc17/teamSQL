package cn.edu.thssdb.index;

import javafx.util.Pair;

public final class BPlusTree<K extends Comparable<K>, V> implements Iterable<Pair<K, V>> {
  // b+树的根节点
  BPlusTreeNode<K, V> root;
  // 关键字的总数
  private int size;
  // 构造函数，初始化根节点（为什么根节点是一个Leaf Node？）
  public BPlusTree() {
    root = new BPlusTreeLeafNode<>(0);
  }
  // size 返回root的size
  public int size() {
    return size;
  }
  // get 获得key的value
  public V get(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to get() is null");
    return root.get(key);
  }
  // update 先移除key再插入key，不是很明白这一步是要干啥，怀疑是删又插会导致树结构更新
  public void update(K key, V value) {
    root.remove(key);
    root.put(key, value);
  }
  // put 插入一个新的关键字
  public void put(K key, V value) {
    if (key == null) throw new IllegalArgumentException("argument key to put() is null");
    root.put(key, value);
    size++;
    checkRoot();
  }
  // remove 移除存在的key，减少size，root必要时变成Internal Node
  public void remove(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to remove() is null");
    root.remove(key);
    size--;
    if (root instanceof BPlusTreeInternalNode && root.size() == 0) {
      root = ((BPlusTreeInternalNode<K, V>) root).children.get(0);
    }
  }
  // contains 查找是否含有key
  public boolean contains(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to contains() is null");
    return root.containsKey(key);
  }
  // checkRoot 处理root超出b+限制时的情况（新的根节点，原节点和新的兄弟节点作为子节点）
  private void checkRoot() {
    if (root.isOverFlow()) {
      BPlusTreeNode<K, V> newSiblingNode = root.split();
      BPlusTreeInternalNode<K, V> newRoot = new BPlusTreeInternalNode<>(1);
      newRoot.keys.set(0, newSiblingNode.getFirstLeafKey());
      newRoot.children.set(0, root);
      newRoot.children.set(1, newSiblingNode);
      root = newRoot;
    }
  }
  // iterator ?
  @Override
  public BPlusTreeIterator<K, V> iterator() {
    return new BPlusTreeIterator<>(this);
  }
}
