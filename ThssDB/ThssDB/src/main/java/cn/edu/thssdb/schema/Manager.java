package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.utils.Global;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  //工作状态的数据库
  private Database workingDb;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<String, Database>();
    try
    {
      String root = System.getProperty("user.dir");
      FileReader fileReader = new FileReader(root+"/data/manager.txt");
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      while ((line = bufferedReader.readLine()) != null)
      {
        Database db = new Database(line);
        databases.put(line, db);
      }
      fileReader.close();
      bufferedReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    //初始化的时候指定一个初始db作为工作db
    //暂定为default
    workingDb = databases.get("default");

  }

  public Database getWorkingDb() {
    return workingDb;
  }

  public void createDatabaseIfNotExists(String name) {
    // TODO
    if(databases.containsKey(name))
    {
      //应该要向客户端返回数据库已经存在
      return;
    }
    else
    {
      Database db = new Database(name);
      databases.put(name, db);

      try
      {
        File file = new File(Global.root+"/data/databases/"+name+".txt");
        file.createNewFile();

        FileWriter fileWriter = new FileWriter(Global.root+"/data/manager.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(name + '\n');
        fileWriter.close();
        bufferedWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void deleteDatabase(String name) {
    // TODO
    if(databases.containsKey(name))
    {
      databases.remove(name);
      try
      {
        //应该要保证数据库中没有表了才能删除
        File file = new File(Global.root+"/data/databases/"+name+".txt");
        file.delete();

        FileWriter fileWriter = new FileWriter(Global.root+"/data/manager.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write("");
        Set<String> keys = databases.keySet();
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext())
        {
          String key = iterator.next();
          bufferedWriter.write(key + "\n");
        }
        fileWriter.close();
        bufferedWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    else
    {
      //向客户端返回没有这个数据库

    }

  }

  public void switchDatabase(String name) {
    // TODO
    workingDb.quit();
    workingDb = databases.get(name);
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
