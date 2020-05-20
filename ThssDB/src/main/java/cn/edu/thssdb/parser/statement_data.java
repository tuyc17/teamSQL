package cn.edu.thssdb.parser;

import java.util.LinkedList;
import java.util.List;

class Column{
    String column_name;
    String type_name;
    List<String> column_constraint = new LinkedList<>();
}
class Table{
    String table_name;
    List<Column> columns= new LinkedList<>();
    String table_constraint;
}
class Condition{
    String comparator;
    String attrName;
    String attValue;
}
class FullColumn{
    String column_name;
    String tableName;
}
class EqualExpression{
    FullColumn column1,column2;
}
public class statement_data{
    String kind; //存储语句类型
    //数据库的创建，删除，切换：
    String database_name; //数据库名称
    //表的创建，删除，展示：
    Table table; //表属性
    //增：
    String table_name; //表名称
    List<String> column_names= new LinkedList<>(); //列名
    List<String> value_entrys= new LinkedList<>(); //列值(大小与上面相等，上面为空则为表的长度)
    //删:
    List<Condition> conditions= new LinkedList<>(); //条件
    //改:
    String column_name; //属性名称
    String expression;// 表达式
    //查：
    List<FullColumn> FullColumns= new LinkedList<>(); //(包括表名的)列名
    List<String> table_names= new LinkedList<>(); //join的表们
    List<EqualExpression> equalexpressions; //用来join的表达式们
}
