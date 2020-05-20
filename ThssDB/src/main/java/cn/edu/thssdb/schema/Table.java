package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import javafx.scene.control.Tab;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;     // 一张表的所有属性的元信息
  public BPlusTree<Entry, Row> index;   // b+树，索引用

  private int primaryIndex;

  // 保存的数据
  private ArrayList<Object[]> data;
  // 具体数据（DBW）
  public ArrayList<Row> rows;

  // 构造函数
  // 创建空表
  public Table(String databaseName, String tableName) throws Exception {
    this(databaseName, tableName, new Column[0], new Object[0][]);
  }

  // 创建指定元信息的空表
  public Table(String databaseName, String tableName, Column[] column) throws Exception {
    this(databaseName, tableName, column, new Object[0][]);
  }

  // 基本构造函数
  public Table(String databaseName, String tableName, Column[] columns, Object[][] data) throws Exception {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<Column>(Arrays.asList(columns));
    this.data = new ArrayList<Object[]>(Arrays.asList(data));
    // Perhaps else to do
  }

  private void recover() {
    // TODO
  }

  public void insert(Object[] rowInsert) throws Exception {
    // TODO
    if(rowInsert.length != columns.size()) {
      throw new Exception("Fatal Error: Lack Attributes");
    }
    for(int i = 0; i < columns.size(); i++) {
      if(columns.get(i).isNotNull() && rowInsert[i] == null)
        throw new Exception("Fatal Error: Not Null Constraint Violated");
    }
    // 还有什么是要判断的吗？

    // 最后插入数据（为了搜索，是否要在这之后更新树？）
    data.add(rowInsert);
  }

  public void delete(int rowDelete) {
    // TODO
    try {
      data.remove(rowDelete);
    }
    catch(Exception e) {

    }
  }

  public void update() {
    // TODO
  }
  //根据网上的教程序列化和反序列化需要implement Serializable接口最后测试的时候可能table类要加上
  private void serialize() throws IOException {
    // TODO
    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(new File("test.txt")));
    out.writeObject(this);
    out.close();
  }
  // 这里框架为什么返回值是ArrayList<Row>即具体数据而不是table类没有想得很清楚
  private ArrayList<Row> deserialize() throws IOException, ClassNotFoundException {
    // TODO
    // return null;
    ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File("test.txt")));
    Table table = (Table)in.readObject();
    in.close();
    return table.rows;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().getValue();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
