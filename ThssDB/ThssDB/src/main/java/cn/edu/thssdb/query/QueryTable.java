package cn.edu.thssdb.query;

import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.parser.EqualExpression;
import cn.edu.thssdb.parser.FullColumn;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryTable implements Iterator<Row> {
  public ArrayList<Row> queryRows;
  public ArrayList<String> attr = new ArrayList<>();
  public ArrayList<ColumnType> attr_type = new ArrayList<>();
  private Iterator<Row> it ;

  public QueryTable(Table t) {
    // TODO
    Iterator<Row> iterator = t.iterator();
    ArrayList<Row> rows = new ArrayList<>();
    while (iterator.hasNext())
    {
      Row row = iterator.next();
      rows.add(row);
    }
    queryRows = rows;
    it = queryRows.iterator();

    for (Column c: t.columns) {
      attr.add(c.getName());
      attr_type.add(c.getType());
    }

  }

  public QueryTable(Table t1, Table t2, List<EqualExpression> equalExpressions) {
    EqualExpression equal_expression = equalExpressions.get(0);
    FullColumn fc1 = equal_expression.column1;
    FullColumn fc2 = equal_expression.column2;

    MetaInfo table1 = new MetaInfo(t1.tableName, t1.columns);
    MetaInfo table2 = new MetaInfo(t2.tableName, t2.columns);

    int index1 = 0;
    int index2 = 0;

    if(t1.tableName.equals(fc1.tableName))
    {
      index1 = table1.columnFind(fc1.column_name);
      index2 = table2.columnFind(fc2.column_name);
    }
    else
    {
      index1 = table1.columnFind(fc2.column_name);
      index2 = table2.columnFind(fc1.column_name);
    }

    Iterator<Row> iterator1 = t1.iterator();
    Iterator<Row> iterator2 = t2.iterator();

    ArrayList<Row> rows = new ArrayList<>();

    while (iterator1.hasNext())
    {
      Row table1_row = iterator1.next();
      ArrayList<Entry> entries1 = table1_row.getEntries();
      while (iterator2.hasNext())
      {
        Row table2_row = iterator2.next();
        ArrayList<Entry> entries2 = table2_row.getEntries();

        if(entries1.get(index1).compareTo(entries2.get(index2)) == 0)
        {
          table2_row.appendEntries(entries1);
          rows.add(table2_row);
        }
      }
      iterator2 = t2.iterator();
    }

    queryRows = rows;
    it = queryRows.iterator();

    for(Column c2: t2.columns)
    {
      if(is_same(c2, t1.columns))
      {
        attr.add(t2.tableName+'.'+c2.getName());
        attr_type.add(c2.getType());
      }
      else
      {
        attr.add(c2.getName());
        attr_type.add(c2.getType());
      }
    }

    for(Column c1: t1.columns)
    {
      if(is_same(c1, t2.columns))
      {
        attr.add(t1.tableName+'.'+c1.getName());
        attr_type.add(c1.getType());
      }
      else
      {
        attr.add(c1.getName());
        attr_type.add(c1.getType());
      }
    }

  }

  private boolean is_same(Column c, ArrayList<Column> columns)
  {
    for(Column i : columns)
    {
      if(c.isSame(i.getName()))
      {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    // TODO
    return it.hasNext();
  }

  @Override
  public Row next() {
    // TODO
    return it.next();
  }
}