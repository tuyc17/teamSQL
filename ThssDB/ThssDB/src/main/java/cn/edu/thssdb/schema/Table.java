package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.type.ColumnType;
import javafx.scene.control.Tab;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;  // 事务锁  /////////////////////////////////////////////////////////////
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;     // 一张表的所有属性的元信息
  public BPlusTree<Entry, Row> index;   // b+树，索引用
  private int primaryIndex;  // 找到columns中主键对应的下标

  // 基本构造函数
  public Table(String databaseName, String tableName, Column[] columns)
  {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<Column>(Arrays.asList(columns));
    for (int i = 0; i < this.columns.size(); i++) {
      if(this.columns.get(i).isPrimary())
      {
        this.primaryIndex = i;
        break;
      }
    }
    recover();
  }

  private void recover() {
    // TODO
    // 通过反序列化得到的rows重新构建b+树
    this.index = deserialize();
  }
  //检查输入的变量类型是否和columns给出的信息一致
  //这里看是直接传entry的数组好
  //还是看发到这里再转化成entry的list
  //entries 还需要变成对应的类型再保存
  public void insert(Entry[] entries) {
    // TODO
    Row row = new Row(entries);
    index.put(entries[primaryIndex], row);
  }

  // 删
  public void delete(List<Condition> conditions) {
    // TODO

    String comparator = ""; // where的操作符
    String left = "";       // 这一行的名字 循环找到名字等于left的行
    String right = "";      //

    switch (comparator)
    {
      case ">":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();    // iterator()：匹配迭代器
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "<":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "<=":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case ">=":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "=":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "<>":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    index.remove(pair.getKey());
                  }
                }
                break;
            }
            break;
          }
        }
        break;
    }
  }

  // 改
  public void update(Condition expression, List<Condition> conditions) {
    // TODO
    String expression_op ="";
    String expression_r ="";
    String expression_l ="";

    int updateIndex = 0;
    for (int i = 0; i < columns.size(); i++)
    {
      if(columns.get(i).isSame(expression_l))
      {
        updateIndex = i;
        break;
      }
    }

    Entry new_entry = null;
    ColumnType columnType = columns.get(updateIndex).getType();
    switch (columnType)
    {
      case INT:
        new_entry = new Entry(Integer.parseInt(expression_r));
        break;
      case LONG:
        new_entry = new Entry(Long.parseLong(expression_r));
        break;
      case FLOAT:
        new_entry = new Entry(Float.parseFloat(expression_r));
        break;
      case DOUBLE:
        new_entry = new Entry(Double.parseDouble(expression_r));
        break;
      case STRING:
        new_entry = new Entry(expression_r);
        break;
    }

    String comparator = "";
    String left = "";
    String right = "";

    switch (comparator)
    {
      case ">":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) > 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "<":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) < 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "<=":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) <= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case ">=":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) >= 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "=":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) == 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
            }
            break;
          }
        }
        break;
      case "<>":
        for(int i = 0; i < columns.size(); i++)
        {
          if(columns.get(i).isSame(left))
          {
            ColumnType t = columns.get(i).getType();
            Entry r;
            BPlusTreeIterator<Entry, Row> iterator;
            switch (t)
            {
              case INT:
                r = new Entry(Integer.parseInt(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case LONG:
                r = new Entry(Long.parseLong(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case FLOAT:
                r = new Entry(Float.parseFloat(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case DOUBLE:
                r = new Entry(Double.parseDouble(right));
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
              case STRING:
                r = new Entry(right);
                iterator = index.iterator();
                while (iterator.hasNext())
                {
                  Pair<Entry, Row> pair = iterator.next();
                  ArrayList<Entry> entries = pair.getValue().getEntries();
                  if(entries.get(i).compareTo(r) != 0 )
                  {
                    Entry[] new_entries = (Entry[]) entries.toArray();
                    new_entries[updateIndex] = new_entry;
                    Row new_row = new Row(new_entries);
                    index.update(pair.getKey(), new_row);
                  }
                }
                break;
            }
            break;
          }
        }
        break;
    }

  }

  //根据网上的教程序列化和反序列化需要implement Serializable接口最后测试的时候可能table类要加上
  //这里应该改成public，在Database类中去调用
  //否则就是每执行一次上面的函数就要调用一次更新文件
  private void serialize() {
    // TODO
    try
    {
      ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(new File("../data/tables/rows/"+tableName+".txt")));
      out.writeObject(this.index);
      out.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  //假设能反序列化成B+树
  //不行就还是返回row的list然后重建B+树
  private BPlusTree<Entry, Row> deserialize() {
    // TODO
    try {
      ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File("../data/tables/rows/"+tableName+".txt")));
      BPlusTree<Entry, Row> tree = (BPlusTree<Entry, Row>)in.readObject();
      in.close();
      return tree;
    }
    catch (ClassNotFoundException | IOException e)
    {
      e.printStackTrace();
    }
    return null;
  }

//  private ArrayList<Row> deserialize() throws IOException, ClassNotFoundException {
//    // TODO
//    return null;
////    ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File("test.txt")));
////    Table table = (Table)in.readObject();
////    in.close();
////    return table.rows;
//  }

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
