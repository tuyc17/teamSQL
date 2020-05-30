package cn.edu.thssdb.index;

import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.Collections;

// b+树节点（包含叶节点和非叶节点），参数是两个类型K和V，其中K是可比较的（关键字的类型），V是(?)
abstract class BPlusTreeNode<K extends Comparable<K>, V> {
  ArrayList<K> keys;  // 关键字列表
  int nodeSize;       // 包含关键字的总数

  abstract V get(K key);  // get 获取关键字key的值(?)

  abstract void put(K key, V value);  // put 放入关键字

  abstract void remove(K key);  // remove 移除关键字

  abstract boolean containsKey(K key);  // containsKey 判断该节点是否包含关键字

  abstract K getFirstLeafKey(); // getFirstLeafKey 获取第一个子节点/叶子节点 (?)

  abstract BPlusTreeNode<K, V> split(); // split 拆分(?)

  abstract void merge(BPlusTreeNode<K, V> sibling); // merge 合并(?) 估计是与split相反的操作
  // size 返回nodeSize，查看节点包含关键字总数的方法
  int size() {
    return nodeSize;
  }
  // isOverFlow 判断当前节点包含的关键字总数是否超过m
  boolean isOverFlow() {
    return nodeSize > Global.fanout - 1;
  }
  // isUnderFlow 判断关键字总数是否低于m/2
  boolean isUnderFlow() {
    return nodeSize < (Global.fanout + 1) / 2 - 1;
  }
  // binarySearch 二分查找一个关键字
  int binarySearch(K key) {
    return Collections.binarySearch(keys.subList(0, nodeSize), key);
  }
  // keysAdd 根据index索引将关键字key加入（i从nodeSize递减至index+1）
  void keysAdd(int index, K key) {
    for (int i = nodeSize; i > index; i--) {
      keys.set(i, keys.get(i - 1)); // ArrayList.set()更新指定下标位置的值
      // 区别：ArrayList.add()在指定位置添加
    }
    keys.set(index, key);
    nodeSize++;
  }
  // keysRemove 将索引index处的关键字移除
  void keysRemove(int index) {
    for (int i = index; i < nodeSize - 1; i++) {
      keys.set(i, keys.get(i + 1));
    }
    nodeSize--;
  }
}
