package cn.edu.thssdb.parser;

import java.util.*;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.type.ColumnType;

public class statement_data {
    public String kind; //存储语句类型
    //数据库的创建，删除，切换：
    public String database_name; //数据库名称
    //表的创建，删除，展示：
    public Table table = new Table(); //表属性
    //增：
    public String table_name; //表名称
    public List<String> column_names = new LinkedList<>(); //列名
    public List<String> value_entrys = new LinkedList<>(); //列值(大小与上面相等，上面为空则为表的长度)
    //删:
    public List<Condition> conditions = new LinkedList<>(); //条件
    //改:
    public Condition expression = new Condition();// 表达式
    //查：
    public List<FullColumn> FullColumns = new LinkedList<>(); //(包括表名的)列名
    public List<String> table_names = new LinkedList<>(); //join的表们
    public List<EqualExpression> equalexpressions = new LinkedList<>(); //用来join的表达式们

    public Column[] getColumns() {
        // 其实是从Table里取
        Column[] temp_list = new Column[Global.MAX_LEN];
        for (int i=0 ;i<table.columns.size();i++){

            cn.edu.thssdb.parser.Column old_column =table.columns.get(i);
            int tempint=0;
            if (old_column.is_Primary_Key){
                tempint =1;
            }

            String str = old_column.type_name;

            int maxLength = Global.MAX_LEN;
            if (str.indexOf("(")!=-1 && str.indexOf(")")!=-1){
                maxLength = Integer.parseInt(str.substring(str.indexOf("(")+1,str.indexOf(")")));
                str = str.substring(0,str.indexOf("("));
            }
            Column temp = new Column(old_column.column_name,ColumnType.valueOf(str.toUpperCase()),tempint,old_column.is_NotNull,maxLength);
            temp_list[i] = temp;
        }
        return temp_list;
    }
}
