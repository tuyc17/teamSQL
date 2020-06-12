package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.schema.Column;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
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

    public void insert(Entry[] entries) {
        // TODO
        Row row = new Row(entries);
        index.put(entries[primaryIndex], row);
        //serialize();
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
        //serialize();
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
        //serialize();
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

    public void serialize() {
        // TODO
        BPlusTreeIterator<Entry, Row> iterator = index.iterator();
        ArrayList<Row> rows = new ArrayList<>();
        while (iterator.hasNext())
        {
            Pair<Entry, Row> pair = iterator.next();
            rows.add(pair.getValue());
        }

        try
        {
            FileOutputStream fo = new FileOutputStream(Global.root + "/data/tables/rows/"+ databaseName + "_"  + tableName + ".txt");
            ObjectOutputStream oo = new ObjectOutputStream(fo);
            oo.writeObject(rows);
            oo.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    private ArrayList<Row> deserialize() {
        // TODO
        try
        {
            File file = new File(Global.root + "/data/tables/rows/"+ databaseName + "_"  + tableName + ".txt");
            if(file.length() == 0)
            {
                return new ArrayList<>();
            }

            FileInputStream fi = new FileInputStream(Global.root + "/data/tables/rows/"+ databaseName + "_"  + tableName + ".txt");
            ObjectInputStream oi = new ObjectInputStream(fi);

            ArrayList<Row> rows = (ArrayList<Row>) oi.readObject();
            oi.close();
            return rows;
        }
        catch (IOException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        return new ArrayList<>();
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
