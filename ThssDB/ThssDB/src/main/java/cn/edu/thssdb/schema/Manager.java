package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.parser.Statement.*;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.thssdb.utils.Global;

public class Manager {
  private static HashMap<String, Database> databases;

  // 有关lock与log
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static Logger logger = new Logger();
  private static ArrayList<Session> sessionList;

  //工作状态的数据库
  public Database workingDb;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<String, Database>();
    try {
      String root = System.getProperty("user.dir");
      FileReader fileReader = new FileReader(root+"/data/manager.txt");
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
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

    logger.redoLog();
  }

  public Database getWorkingDb() {
    try {
      lock.readLock().lock();
      return workingDb;
    } finally {
      lock.readLock().unlock();
    }
  }

  public static boolean createDatabaseIfNotExists(String name) {
    // TODO
    if(databases.containsKey(name))
    {
      //应该要向客户端返回数据库已经存在
      return false;
    }
    else
    {
      try
      {
        File file = new File(Global.root+"/data/databases/"+name+".txt");
        file.createNewFile();
        FileWriter fileWriter = new FileWriter(Global.root+"/data/manager.txt",true);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(name + '\n');
        bufferedWriter.flush();
        bufferedWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      Database db = new Database(name);
      databases.put(name, db);
      return true;
    }
  }

  public static int deleteDatabase(String name) {
    // TODO
    // 返回0：成功 1:数据库不存在 2:数据库非空
    if(databases.containsKey(name))
    {
      databases.remove(name);
      try
      {
        //TODO
        //应该要保证数据库中没有表了才能删除
        File file = new File(Global.root+"/data/databases/"+name+".txt");
        if (file.length()!=0){
          return 2;
        }
        file.delete();

        FileWriter fileWriter = new FileWriter(Global.root+"/data/manager.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        Set<String> keys = databases.keySet();
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext())
        {
          String key = iterator.next();
          bufferedWriter.write(key + "\n");
        }
        bufferedWriter.flush();
        bufferedWriter.close();
        return 0;

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    else
    {
      //向客户端返回没有这个数据库
      return 1;
    }
    return 3;
  }

  public boolean switchDatabase(String name) {
    // TODO
    Database temp = databases.get(name);
    if (temp==null){
      return false;
    }
    else{
      workingDb.quit();
      workingDb = databases.get(name);
      return true;
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }

  // tuyc's implement functions of rpc.thrift
  private static Session getSession(long sessionId) {
    for (Session session: sessionList) {
      if (session.sessionId == sessionId) {
        return session;
      }
    }
    return null;
  }

  public static int setAutoCommit(boolean autoCommit, long sessionId) {
    Session session = getSession(sessionId);
    if (session == null) {
      return 0;
    }
    else {
      if (session.inTransaction) {
        return 2;
      }
      session.autoCommit = autoCommit;
      return 1;
    }
  }

  public static int beginTransaction(long sessionId) {
    Session session = getSession(sessionId);
    if (session == null) {
      return 0;
    }
    else {
      if (session.inTransaction) {
        return 2;
      }
      session.inTransaction = true;
      return 1;
    }
  }

  public static int commit(long sessionId) {
    Session session = getSession(sessionId);
    if (session == null) {
      return 0;
    }
    else {
      for (ReentrantReadWriteLock lock: session.lockList) {
        lock.writeLock().unlock();
      }
      session.lockList.clear();
      logger.commitLog(session.logList);
      session.logList.clear();
      session.inTransaction = false;
      return 1;
    }
  }

  // tuyc's functional functions
  private static Database getDatabase(Session session) {
    try {
      lock.readLock().lock();
      if (session.currentDatabase == null) {
        throw new RuntimeException();
      }
      else {
        return databases.get(session.currentDatabase);
      }
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private static Database getDatabase(String name) {
    try {
      lock.readLock().lock();
      Database database = databases.get(name);
      if (database == null) {
        throw new RuntimeException();
      }
      return database;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private static void lockAll() {
    lock.writeLock().lock();
    // make sure all the child locks released by other transaction
    for (Database database: databases.values()) {
      if (database == null)
        continue;
      database.lock.writeLock().lock();
      for (Table table: database.tables.values()) {
        if (table == null)
          continue;
        table.lock.writeLock().lock();
        table.lock.writeLock().unlock();
      }
      database.lock.writeLock().unlock();
    }
  }

  private static void lockDatabase(Database database) {
    lock.writeLock().lock();
    database.lock.writeLock().lock();
    // make sure the child locks of the database released by other transaction
    for (Table table: database.tables.values()) {
      if (table == null) continue;
      table.lock.writeLock().lock();
      table.lock.writeLock().unlock();
    }
    lock.writeLock().unlock();
  }

  private static void lockTable(Database database, Table table) {
    lock.writeLock().lock();
    database.lock.writeLock().lock();
    table.lock.writeLock().lock();
    database.lock.writeLock().unlock();
    lock.writeLock().unlock();
  }

  private static boolean managerPersist() {
    try {
      lockAll();
      File dir = new File("data");
      if (dir.exists()) {
        Path databaseDirector = Paths.get(dir.toString());
        Files.walkFileTree(databaseDirector, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
      }
      if (!dir.mkdirs()) {
        System.err.println("Fail to persist due to mkdirs error!");
        return false;
      }
      ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+"DATABASES_NAME"));
      for (String databaseName: databases.keySet()) {
        oos.writeObject(databaseName);
      }
      for (Database database: databases.values()) {
        if (!database.persist()) {
          return false;
        }
      }
      return true;
    }
    catch (FileNotFoundException e) {
      System.err.print("Fail to persist manager due to FileNotFoundException!");
      return false;
    }
    catch (IOException e) {
      System.err.print("Fail to persist manager due to IOException!");
      return false;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  private static class Logger {
    private ReentrantReadWriteLock lock;
    private int logCnt; // 计数log的数量
    private int flushThreshold;  // 缓冲阈值

    private Logger() {
      this.lock = new ReentrantReadWriteLock();
      this.logCnt = 0;
      this.flushThreshold = 5;
    }
    // 向log中添加各种各样的操作。应该在sqlExecutor中调用他们
    private void addCreateDatabase(ArrayList<String> logList, String dbName) {
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.CREATE_DATABASE.toString());
      log.add(dbName);
      logList.add(String.join("|", log));
    }

    private void addDropDatabase(ArrayList<String> logList, String dbName) {
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.DROP_DATABASE.toString());
      log.add(dbName);
      logList.add(String.join("|", log));
    }

    private void addCreateTable(ArrayList<String> logList, String dbName, String tableName, ArrayList<Column> columnsList) {
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.CREATE_TABLE.toString());
      log.add(dbName);
      log.add(tableName);
      for (Column column: columnsList) {
        log.add(column.toString());
      }
      logList.add(String.join("|", log));
    }

    private void addDropTable(ArrayList<String> logList, String dbName, String tableName) {
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.DROP_TABLE.toString());
      log.add(dbName);
      log.add(tableName);
      logList.add(String.join("|", log));
    }

    private void addInsert(ArrayList<String> logList, String dbName, String tableName, Row row) {
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.INSERT.toString());
      log.add(dbName);
      log.add(tableName);
      log.add(row.toString());
      logList.add(String.join("|", log));
    }

    private void addDelete(ArrayList<String> logList, String databaseName, String tableName, ArrayList<Row> row2Delete) {
      if (row2Delete.size() == 0) {
        return;
      }
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.DELETE.toString());
      log.add(databaseName);
      log.add(tableName);
      for (Row row: row2Delete) {
        log.add(row.toString());
      }
      logList.add(String.join("|", log));
    }

    private void addDelete(ArrayList<String> logList, String databaseName, String tableName, Row row) {
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.DELETE.toString());
      log.add(databaseName);
      log.add(tableName);
      log.add(row.toString());
      logList.add(String.join("|", log));
    }

    private void addUpdate(ArrayList<String> logList, String databaseName, String tableName, ArrayList<Row> rowUpdated) {
      if (rowUpdated.size() == 0) {
        return;
      }
      ArrayList<String> log = new ArrayList<>();
      log.add(Statement.Type.UPDATE.toString());
      log.add(databaseName);
      log.add(tableName);
      for (Row row: rowUpdated) {
        log.add(row.toString());
      }
      logList.add(String.join("|", log));
    }

    private void commitLog(ArrayList<String> logList) {
      try {
        lock.writeLock().lock();
        File dir = new File("data");
        if (!dir.exists() && !dir.mkdirs()) {
          System.err.println("Fail to write log due to mkdirs error!");
          return;
        }
        // 在data/log里准备一个log文件
        FileWriter fileWriter = new FileWriter(dir.toString() + File.separator + "log", true);
        // 向log文件里写log
        for (String string: logList) {
          fileWriter.write(string+'\n');
          logCnt++;
        }
        fileWriter.flush();
        fileWriter.close();
        System.out.println(logCnt);
        if (logCnt >= this.flushThreshold && managerPersist()) {
          logCnt = 0;
          File logFile = new File("data" + File.separator + "log");
          if (logFile.exists()) {
            logFile.delete();
          }
        }
      } catch (IOException ignored) {
        // throw new WriteLogException();
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void redoLog() {
      try {
        lock.writeLock().lock();
        File file = new File("data" + File.separator + "log");
        if (!file.exists()) {
          return;
        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
          logCnt++;
          String[] log = line.split("\\|");
          switch(Statement.Type.valueOf(log[0])) {
            case CREATE_DATABASE:
              redoCreateDatabase(log);
              break;
            case DROP_DATABASE:
              redoDropDatabase(log);
              break;
            case CREATE_TABLE:
              redoCreateTable(log);
              break;
            case DROP_TABLE:
              redoDropTable(log);
              break;
            case INSERT:
              redoInsert(log);
              break;
            case DELETE:
              redoDelete(log);
              break;
            case UPDATE:
              redoUpdate(log);
              break;
            default:
              System.err.println("Error: unknown log type!");
              break;
          }
        }
      } catch (IOException ignored) {
        // nothing to do
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void redoCreateDatabase(String[] log) {
      try {
        Manager.createDatabaseIfNotExists(log[1]);
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    private void redoDropDatabase(String[] log) {
      try {
        Manager.deleteDatabase(log[1]);
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    private void redoCreateTable(String[] log) {
      try {
        Database database = Manager.getDatabase(log[1]);
        ArrayList<Column> columnsList = new ArrayList<>();
        for (int i = 3;i < log.length;i++) {
          columnsList.add(Column.parseColumnDef(log[i]));
        }
        Column[] columnsArray = (Column[])columnsList.toArray(new Column[columnsList.size()]);
        database.create(log[2], columnsArray);
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    private void redoDropTable(String[] log) {
      try {
        Database database = Manager.getDatabase(log[1]);
        database.drop(log[2]);
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    private void redoInsert(String[] log) {
      try {
        Database database = Manager.getDatabase(log[1]);
        Table table = database.getTable(log[2]);
        table.insert(Row.parseRowDef(log[3], table.columns));
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    private void redoDelete(String[] log) {
      try {
        Database database = Manager.getDatabase(log[1]);
        Table table = database.getTable(log[2]);
        table.delete(Row.parseRowDef(log[3], table.columns));
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    private void redoUpdate(String[] log) {
      try {
        Database database = Manager.getDatabase(log[1]);
        Table table = database.getTable(log[2]);
        for (int i = 3;i < log.length;i++) {
          table.update(Row.parseRowDef(log[i], table.columns));
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }
  }
}

// tuyc's extra class
class Session {
  long sessionId;
  boolean autoCommit;
  boolean inTransaction;
  ArrayList<String> logList;
  ArrayList<ReentrantReadWriteLock> lockList;
  String currentDatabase;

  Session(long sessionId) {
    this.sessionId = sessionId;
    this.autoCommit = true;
    this.inTransaction = false;
    this.logList = new ArrayList<>();
    this.lockList = new ArrayList<>();
    this.currentDatabase = null;
  }
}
