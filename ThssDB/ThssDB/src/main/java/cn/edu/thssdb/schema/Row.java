package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null)
      return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries)
      sj.add(e.toString());
    return sj.toString();
  }

  // tuyc
  public static Row parseRowDef(String attrStr, ArrayList<Column> columnsList) {
    String[] attrListStr = attrStr.split(",");
    if (attrListStr.length != columnsList.size()) {
      throw new RuntimeException();
    }
    ArrayList<Entry> entryList = new ArrayList<>();
    for (int i = 0;i < attrListStr.length;i++) {
      switch (columnsList.get(i).getType()) {
        case INT:
        case LONG:
          entryList.add(new Entry(Long.valueOf(attrListStr[i])));
          break;
        case FLOAT:
        case DOUBLE:
          entryList.add(new Entry(Double.valueOf(attrListStr[i])));
          break;
        case STRING:
          entryList.add(new Entry(attrListStr[i]));
          break;
        default:
          break;
      }
    }
    Entry[] entryArray = entryList.toArray(new Entry[0]);
    return new Row(entryArray);
  }
}
