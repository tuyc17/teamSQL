package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

// 每列的元信息
public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  public String getName() {
    return name;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public boolean isNotNull() { return notNull; }

  public boolean isPrimary() { return primary == 1; }

  public boolean isSame(String n) { return name.equals(n); }

  public ColumnType getType() { return type; }

  // tuyc
  static Column parseColumnDef(String defStr) {
    String[] defListStr = defStr.split(",");
    return new Column(defListStr[0], // name
            ColumnType.valueOf(defListStr[1]),  // ColumnType
            Integer.parseInt(defListStr[2]),  // primary
            defListStr[3].equals("true"), // notNull
            Integer.parseInt(defListStr[2])); // maxLength
  }
}
