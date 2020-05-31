package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.io.*;
import java.util.*;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.utils.Global;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  // ReentrantReadWriteLock:
  // 可重入（每次加锁count+1）
  // 读写分离：写数据和读数据分开，加上两把不同的锁
  // 锁降级：线程获取写入锁后可以获取读取锁，然后释放写入锁，实现写锁->读锁
  // 有效避免锁升级：不会出现读锁->写锁
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }
  // 字符串转Entry
  public static Entry GetEntry(ColumnType type,String temp){
      Entry ret = null;
      switch (type){
        case STRING:
          ret = new Entry(temp);
          break;
        case INT:
          ret = new Entry(Integer.parseInt(temp));
          break;
        case LONG:
          ret = new Entry(Long.parseLong(temp));
          break;
        case FLOAT:
          ret = new Entry(Float.parseFloat(temp));
          break;
        case DOUBLE:
          ret = new Entry(Double.parseDouble(temp));
          break;
      }
      return ret;
  }
  //B+树转list<list<String>>
  public static List<List<String>> BTreeParseLLS(BPlusTree<Entry, Row> index){
    List<List<String>> ret= new ArrayList<>();
    BPlusTreeIterator<Entry, Row> iterator=index.iterator();
    while (iterator.hasNext())
    {
      Pair<Entry, Row> pair = iterator.next();
      ArrayList<Entry> entries = pair.getValue().getEntries();
      List<String> temp_list = new ArrayList<>();
      for (int i=0;i<entries.size();i++){
        String str = String.valueOf(entries.get(i));
        if (str.equals("null")){
          break;
        }
        temp_list.add(String.valueOf(entries.get(i)));
      }
      ret.add(temp_list);
    }
    return ret;
  }
  public HashMap<String, Table> getTables() {
    return tables;
  }

  //这里实现了两种方法
  //一个是每次操作都直接写入文件
  //一个是quit执行时再写入文件
  //具体测试看哪个更好
  private void persist() {
    // TODO
    ArrayList<String> old_keys = new ArrayList<>();
    Set<String> new_keys = tables.keySet();
    Iterator<String> iterator = new_keys.iterator();

    ArrayList<String> notChange = new ArrayList<>();

    try
    {
      FileReader fileReader = new FileReader(Global.root+"/data/databases/"+name+".txt");
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      while ((line = bufferedReader.readLine()) != null)
      {
        old_keys.add(line);
      }
      //比较老键和新键得到那些表被删了，那些表是新建的
      while (iterator.hasNext())
      {
        String key = iterator.next();
        for (String s:old_keys) {
          if(s.equals(key))
          {
            notChange.add(s);
          }
        }
      }
      //old_keys除去notChange得到被删除的
      old_keys.removeAll(notChange);
      //new_keys除去notChange得到新增的
      new_keys.removeAll(notChange);
      //删除已经删除的表的文件
      for (String s: old_keys) {
        File file = new File(Global.root+"/data/tables/columns/"+s+".txt");
        file.delete();
        file = new File(Global.root+"/data/tables/rows/"+s+".txt");
        file.delete();
      }
      //创建已经创建的表的文件
      for (String s: new_keys) {
        //创建元信息文件
        File file = new File(Global.root+"/data/tables/columns/"+s+".txt");
        file.createNewFile();
        //写元信息
        Table table = tables.get(s);
        FileWriter fileWriter = new FileWriter(Global.root+"/data/tables/columns/"+s+".txt");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        for (Column c: table.columns) {
          bufferedWriter.write(c.toString()+"\n");
        }
        fileWriter.close();
        bufferedWriter.close();
        //创建实际数据文件
        file = new File(Global.root+"/data/tables/rows/"+s+".txt");
        file.createNewFile();
        //写实际数据
        //在这里调用Table类中的persist函数
        //TODO

      }
      //更新数据库文件
      FileWriter fileWriter = new FileWriter(Global.root+"/data/databases/"+name+".txt");
      BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
      bufferedWriter.write("");
      for(String s : new_keys)
      {
        bufferedWriter.write(s+'\n');
      }
      fileWriter.close();
      bufferedWriter.close();
      fileReader.close();
      bufferedReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void create(String tableName, Column[] columns) {
    // TODO
    if(tables.containsKey(tableName))
    {
      //告知客户端表存在
      return;
    }
    else
    {
      try
      {
        // 向数据库的文件中增加表的名字

        FileWriter fileWriter = new FileWriter(Global.root+"/data/databases/"+name+".txt");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(tableName+"\n");
        bufferedWriter.flush();
        bufferedWriter.close();

        //创建表的元数据和实际数据
        File file = new File(Global.root+"/data/tables/columns/"+tableName+".txt");
        file.createNewFile();
        fileWriter = new FileWriter(Global.root+"/data/tables/columns/"+tableName+".txt");
        bufferedWriter = new BufferedWriter(fileWriter);
        for (Column c: columns) {
          System.out.println(c.toString());
          bufferedWriter.write(c.toString()+"\n");
        }
        //先关buffer
        bufferedWriter.flush();
        bufferedWriter.close();

        file = new File(Global.root+"/data/tables/rows/"+tableName+".txt");
        file.createNewFile();
      }
      catch (IOException e) {
        e.printStackTrace();
      }

      Table table = new Table(this.name, tableName, columns);
      tables.put(tableName, table);

    }
  }

  public void drop(String tableName) {
    // TODO
    if (tables.containsKey(tableName))
    {
      tables.remove(tableName);
      try
      {
        //删除数据库文件中对应表的名字
        FileWriter fileWriter = new FileWriter(Global.root+"/data/databases/"+name+".txt");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write("");
        Set<String> keys = tables.keySet();
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext())
        {
          String key = iterator.next();
          bufferedWriter.write(key+"\n");
        }
        fileWriter.close();
        bufferedWriter.close();
        //删除表对应的文件
        File file = new File(Global.root+"/data/tables/columns/"+tableName+".txt");
        file.delete();
        file = new File(Global.root+"/data/tables/rows/"+tableName+".txt");
        file.delete();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
    }
    else
    {
      //告诉客户端表不存在
    }
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);

    return null;
  }

  private void recover() {
    // TODO
    try
    {
      //读数据库的文件得到数据库中所有表的名字并重新创建
      FileReader fileReader = new FileReader(Global.root+"/data/databases/"+name+".txt");
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      while ((line = bufferedReader.readLine()) != null)
      {
        //读每个表的元信息文件还原成columns
        FileReader fileReader1 = new FileReader(Global.root+"/data/tables/columns/"+line+".txt");
        BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
        String line1;
        ArrayList<Column> columnArrayList = new ArrayList<>();

        while ((line1 = bufferedReader1.readLine()) != null)
        {
          String[] attr = line1.split(",");
          ColumnType type;
          switch (attr[1])
          {
            case "INT":
              type = ColumnType.INT;
              break;
            case "LONG":
              type = ColumnType.LONG;
              break;
            case "FLOAT":
              type = ColumnType.FLOAT;
              break;
            case "DOUBLE":
              type = ColumnType.DOUBLE;
              break;
            case "STRING":
              type = ColumnType.STRING;
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + attr[1]);
          }
          boolean bool;
          bool = attr[3].equals("true");

          Column c = new Column(attr[0], type, Integer.parseInt(attr[2]), bool, Integer.parseInt(attr[4]));
          columnArrayList.add(c);
        }
        bufferedReader1.close();
        fileReader1.close();
        Table table = new Table(name, line, (Column[])columnArrayList.toArray(new Column[0]));
        tables.put(line, table);
      }
      fileReader.close();
      bufferedReader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
//当数据库切换以及服务器关闭时调用这个函数
  public void quit() {
    // TODO
    //数据持久化
    //persist();
  }
}
