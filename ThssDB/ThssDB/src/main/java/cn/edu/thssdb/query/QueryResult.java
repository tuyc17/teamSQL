package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.Condition;
import cn.edu.thssdb.schema.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cn.edu.thssdb.type.ColumnType;
import javafx.scene.control.Cell;
import javafx.util.Pair;


public class QueryResult {

    public List<String> columnName;
    public List<List<Entry>> entry = new ArrayList<>();


    public QueryResult(QueryTable queryTables, List<Condition> conditionList) {
        // TODO
        // 初始化
        this.columnName = queryTables.attr;
        List<List<Entry>> preList = new ArrayList<>();
        for (int i = 0; i < queryTables.queryRows.size(); i++) {
            preList.add(queryTables.queryRows.get(i).getEntries());
        }
        // 只处理and操作符(应付展示)\笑脸
        for (int i = 0; i < conditionList.size(); i++) {
            String comparator = conditionList.get(i).comparator;
            String left = conditionList.get(i).left;
            String right = conditionList.get(i).right;
            int index = 0;
            for (Row row : queryTables.queryRows) {

                for (int j = 0; j < columnName.size(); j++) {
                    if (!columnName.get(j).equals(left)) {
                        continue;
                    }
                    ColumnType t = queryTables.attr_type.get(j);
                    Entry l = row.getEntries().get(j);
                    Entry r = Database.GetEntry(t, right);
                    if (l.value.equals("null")||r.value.equals("null")){
                        continue;
                    }
                    if (Table.isSatisfied(comparator, l, r)) {
                        entry.add(preList.get(index));
                    }

                }
                index += 1;
            }
            preList = deepCopy(entry);
        }
    }

    public static List<List<Entry>> deepCopy(List<List<Entry>> b) {
        List<List<Entry>> a = new ArrayList<>();
        for (int i = 0; i < b.size(); i++) {
            a.add(b.get(i));
        }
        return a;
    }

}