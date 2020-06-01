package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.schema.Column;
import javafx.scene.control.Tab;
import javafx.util.Pair;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;  // 事务锁
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;     // 一张表的所有属性的元信息

    public BPlusTree<Entry, Row> index = new BPlusTree<>();   // b+树，索引用
    private int primaryIndex;  //columns中主键的下标

    // 基本构造函数
    public Table(String databaseName, String tableName, Column[] columns) {
        // TODO
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<Column>(Arrays.asList(columns));
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).isPrimary()) {
                this.primaryIndex = i;
                break;
            }
        }
        recover();
    }

    public List<String> GetColumnName() {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ret.add(columns.get(i).getName());
        }
        return ret;
    }

    // tuyc:同Database.persist，反正你们不用，不如拿来我用（
    boolean persist() {
        try {
            lock.writeLock().lock();
            // 下面这个try-catch不太妙，是我自己实现的serialize，恐怕要出问题
            try {
                File dir = new File("data" + File.separator + databaseName + File.separator + "data");
                if (!dir.exists() && !dir.mkdirs()) {
                    System.err.print("Fail to serialize due to mkdirs error!");
                    return false;
                }
                ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString() + File.separator + tableName));
                for (Row row : this) {
                    oos.writeObject(row);
                }
                oos.close();
                return true;
            } catch (IOException e) {
                System.err.print("Fail to serialize due to IOException!");
                return false;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recover() {
        // TODO
        ArrayList<Row> rows = deserialize();
        for (Row r : rows) {
            ArrayList<Entry> entries = r.getEntries();
            index.put(entries.get(primaryIndex), r);
        }
    }

    //检查输入的变量类型是否和columns给出的信息一致
    //这里看是直接传entry的数组好
    //还是看发到这里再转化成entry的list
    //entries 还需要变成对应的类型再保存
    public void insert(Entry[] entries) {
        // TODO
        Row row = new Row(entries);
        index.put(entries[primaryIndex], row);

        serialize();
    }

    // tuyc's functional functions
    boolean checkRowExist(Entry primary) {
        try {
            lock.readLock().lock();
            return index.contains(primary);
        } finally {
            lock.readLock().unlock();
        }
    }

    // tuyc's override
    void insert(Row row) throws DuplicateKeyException {
        // TODO
        try {
            lock.writeLock().lock();
            Entry primary = row.getEntries().get(primaryIndex);
            if (checkRowExist(primary)) {
                throw new DuplicateKeyException();
            }
            index.put(primary, row);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Pair<Entry, Row>>  iteratorModel(String left, String comparator, String right) {
        //将表中符合条件的行都取出来
        List<Pair<Entry, Row>> ret = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isSame(left)) {
                ColumnType t = columns.get(i).getType();
                Entry r = Database.GetEntry(t, right);
                for (Pair<Entry, Row> pair : index) {
                    ArrayList<Entry> entries = pair.getValue().getEntries();
                    if (isSatisfied(comparator, entries.get(i), r)) {
                        ret.add(pair);
                    }
                }
            }
        }
        return ret;
    }


    public void delete_unit(Pair<Entry, Row> pair) {
        index.remove(pair.getKey());
    }

    public void update_unit(Pair<Entry, Row> pair, String left, String right) {
        int updateIndex = 0;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isSame(left)) {
                updateIndex = i;
                break;
            }
        }
        ArrayList<Entry> entries = pair.getValue().getEntries();
        Entry[] new_entries =  entries.toArray(new Entry[0]);
        ColumnType t = columns.get(updateIndex).getType();
        new_entries[updateIndex] = Database.GetEntry(t, right);
        Row new_row = new Row(new_entries);
        index.update(pair.getKey(), new_row);
    }

    public static boolean isSatisfied(String comparator, Entry temp, Entry r) {
        switch (comparator) {
            case ">":
                return temp.compareTo(r) > 0;
            case "<":
                return temp.compareTo(r) < 0;
            case ">=":
                return temp.compareTo(r) >= 0;
            case "<=":
                return temp.compareTo(r) <= 0;
            case "=":
                return temp.compareTo(r) == 0;
            default:
                System.out.println("解析错误！");
                return false;
        }
    }


    public void delete(List<cn.edu.thssdb.parser.Condition> conditions) {
        // TODO
        // 暂时只操作一个
        String comparator = conditions.get(0).comparator;
        String left = conditions.get(0).left;
        String right = conditions.get(0).right;
        List<Pair<Entry, Row>> templist = iteratorModel(left, comparator, right);
        for (Pair<Entry, Row> pair :templist){
            delete_unit(pair);
        }
        serialize();
    }

    // tuyc's override
    void delete(Row row) {
        // TODO
        try {
            lock.writeLock().lock();
            Entry primary = row.getEntries().get(primaryIndex);
            if (!checkRowExist(primary)) {
                throw new KeyNotExistException();
            }
            index.remove(primary);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void update(cn.edu.thssdb.parser.Condition expression, List<cn.edu.thssdb.parser.Condition> conditions) {
        // TODO
        String comparator = conditions.get(0).comparator;
        String left = conditions.get(0).left;
        String right = conditions.get(0).right;
        List<Pair<Entry, Row>> templist = iteratorModel(left, comparator, right);
        for (Pair<Entry, Row> pair :templist){
            update_unit(pair,expression.left,expression.right);
        }
        serialize();
    }

    // tuyc's override
    void update(Row row) {
        // TODO
        try {
            lock.writeLock().lock();
            Entry entry = row.getEntries().get(primaryIndex);
            if (!index.contains(entry))
                throw new KeyNotExistException();
            index.update(entry, row);
        } finally {
            lock.writeLock().unlock();
        }
    }

    //根据网上的教程序列化和反序列化需要implement Serializable接口最后测试的时候可能table类要加上
    //这里应该改成public，在Database类中去调用
    //否则就是每执行一次上面的函数就要调用一次更新文件
    private void serialize() {
        // TODO
        try {
            FileWriter fileWriter = new FileWriter(Global.root + "/data/tables/rows/" + tableName + ".txt");
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            BPlusTreeIterator<Entry, Row> iterator = index.iterator();
            while (iterator.hasNext()) {
                Pair<Entry, Row> pair = iterator.next();
                ArrayList<Entry> entries = pair.getValue().getEntries();
                for (int i = 0; i < entries.size(); i++) {
                    if (i == entries.size() - 1) {
                        bufferedWriter.write(entries.get(i).toString());
                    } else {
                        bufferedWriter.write(entries.get(i).toString() + ",");
                    }
                }
                bufferedWriter.write("\n");
            }

            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //假设能反序列化成B+树
    //不行就还是返回row的list然后重建B+树
    private ArrayList<Row> deserialize() {
        // TODO
        try {
            FileReader fileReader = new FileReader(Global.root + "/data/tables/rows/" + tableName + ".txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            ArrayList<Row> rows = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                String[] values = line.split(",");
                ArrayList<Entry> entries = new ArrayList<>();
                for (int i = 0; i < values.length; i++) {
                    ColumnType type = columns.get(i).getType();
                    Entry entry;
                    switch (type) {
                        case INT:
                            entry = new Entry(Integer.parseInt(values[i]));
                            entries.add(entry);
                            break;
                        case LONG:
                            entry = new Entry(Long.parseLong(values[i]));
                            entries.add(entry);
                            break;
                        case FLOAT:
                            entry = new Entry(Float.parseFloat(values[i]));
                            entries.add(entry);
                            break;
                        case DOUBLE:
                            entry = new Entry(Double.parseDouble(values[i]));
                            entries.add(entry);
                            break;
                        case STRING:
                            entry = new Entry(values[i]);
                            entries.add(entry);
                            break;
                    }
                }
                Row row = new Row(entries.toArray(new Entry[0]));
                rows.add(row);
            }
            return rows;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
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
