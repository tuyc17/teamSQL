package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.statement_data;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import cn.edu.thssdb.parser.SQLLexer;
import java.util.Date;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.locks.Condition;

import cn.edu.thssdb.parser.mySQLvisitor;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.schema.Manager;

public class IServiceHandler implements IService.Iface {
    public ThssDB server;

    @Override
    public GetTimeResp getTime(GetTimeReq req) throws TException {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) throws TException {

        // TODO
        long sessionId=server.new_session();
        System.out.println("Test:");
        System.out.println(req.username);
        System.out.println(req.password);

        ConnectResp resp = new ConnectResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        resp.setSessionId(sessionId);
        System.out.println(resp.sessionId);
        return resp;
    }

    @Override
    public DisconnectResp disconnect(DisconnectReq req) throws TException {
        // TODO
        DisconnectResp resp = new DisconnectResp();
        server.remove_session(req.sessionId);
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        resp.status.msg="成功断开连接，欢迎再次使用";
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
        Table tempTable,table;
        boolean success;
        int retCase;
        long session =req.sessionId;
        CharStream input = CharStreams.fromString(req.statement.toLowerCase());
        //转成小写以规避大小写问题
        SQLLexer lexer = new SQLLexer(input);
        //此处截取输出日志
        PrintStream oldPrintStream = System.err; //将原来的System.out交给printStream 对象保存
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setErr(new PrintStream(bos));//设置新的out
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseTree tree = parser.sql_stmt_list(); // parse
        mySQLvisitor visitor =new mySQLvisitor();
        statement_data t = visitor.visit(tree);
        ExecuteStatementResp resp = new ExecuteStatementResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        if (t==null){
            //语法错误
            resp.status.code=Global.FAILURE_CODE;
            resp.status.msg =bos.toString();
            System.setErr(oldPrintStream);//恢复原来的System.out
            return resp;
        }
        System.setErr(oldPrintStream);//恢复原来的System.out
        System.out.print("收到信息，类型为:");
        System.out.print(t.kind);
        //在此处语法解析完成，并生成 statement_data t，请对t进行访问，以修改数据库
        // TODO 处理数据库
        // 忽略异常处理

        Database db = server.manager.getWorkingDb();
        switch (t.kind){
            case "use_database":
                //切换数据库
                //已测试
                success = server.manager.switchDatabase(t.database_name);
                if (success){
                    resp.status.code=Global.SUCCESS_CODE;
                    resp.getStatus().msg="切换数据库成功";
                }
                else{
                    resp.status.code=Global.FAILURE_CODE;
                    resp.status.msg="该数据库不存在";
                }

                //异常状况:1.切换的数据库不存在
                break;
            case "create_database":
                //已测试
                success =server.manager.createDatabaseIfNotExists(t.database_name);
                if (success){
                    resp.status.code=Global.SUCCESS_CODE;
                    resp.getStatus().msg="创建数据库成功";
                }
                else{
                    resp.status.code=Global.FAILURE_CODE;
                    resp.status.msg="创建数据库失败:已存在同名数据库";
                }
                //异常状况:1.试图创建的数据库已存在
                break;
            case "drop_database":
                //已测试
                retCase =server.manager.deleteDatabase(t.database_name);
                switch (retCase){
                    case 0:
                        resp.status.code=Global.SUCCESS_CODE;
                        resp.getStatus().msg="删除数据库成功";
                        break;
                    case 1:
                        resp.status.code=Global.FAILURE_CODE;
                        resp.getStatus().msg="删除数据库失败:数据库不存在";
                        break;
                    case 2:
                        resp.status.code=Global.FAILURE_CODE;
                        resp.getStatus().msg="删除数据库失败:数据库非空";
                        break;
                    default:
                        break;
                }
                //异常状况:1.数据库不存在
                //        2.数据库非空
                break;
            case "create_table":
                //已测试
                success = db.create(t.table.table_name,t.getColumns());
                if (success){
                    resp.status.code=Global.SUCCESS_CODE;
                    resp.getStatus().msg="创建表成功";
                }
                else{
                    resp.status.code=Global.FAILURE_CODE;
                    resp.status.msg="创建表失败:已存在同名表";
                }
                //异常状况:1.试图创建的表已存在
                break;
            case "insert":
                //已测试
                //错误情况尚未处理(多主键相同)
                table = db.getTables().get(t.table_name);
                Entry[] temp_list = new Entry[table.columns.size()];
                //找每个名字对应的属性
                if (t.column_names.size()==0){
                    // TODO：可空问题
                    for (int j=0;j<table.columns.size();j++){
                        if (t.value_entrys.get(j)!=null){
                            temp_list[j] = Database.GetEntry(table.columns.get(j).getType(),t.value_entrys.get(j));
                        }
                    }
                }
                else{
                    for (int i=0;i<t.column_names.size();i++){
                        //其中可以搞点异常处理
                        for (int j=0;j<table.columns.size();j++){
                            if (t.column_names.get(i).equals(table.columns.get(j).getName())){
                                //第j个属性值应为第i个属性
                                temp_list[j] = Database.GetEntry(table.columns.get(j).getType(),t.value_entrys.get(i));
                            }
                        }
                    }
                }
                //然后看看有没有可空属性空
                for (int i=0;i<table.columns.size();i++){
                    if (temp_list[i]==null &&  table.columns.get(i).isNotNull()){
                        //不可空可空，报错
                        resp.getStatus().msg="插入数据失败,原因:不可空数据缺失";
                        resp.getStatus().code=Global.FAILURE_CODE;
                        break;
                    }
                }
                try {
                table.insert(temp_list);
                }
                catch (cn.edu.thssdb.exception.DuplicateKeyException e){
                    resp.getStatus().msg="已存在相同的主键";
                    resp.getStatus().code=Global.FAILURE_CODE;
                    break;
                }
                //插入成功后，返回插入后的表情况
                //将表信息输出给client
                tempTable= db.getTables().get(t.table_name);
                //输出列
                resp.columnsList = tempTable.GetColumnName();
                //输出行
                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.getStatus().msg="插入数据成功";
                break;
            case "delete":
                table = db.getTables().get(t.table_name);
                table.delete(t.conditions);
                //删除成功后，返回插入后的表情况
                //将表信息输出给client
                tempTable= db.getTables().get(t.table_name);
                //输出列
                resp.columnsList = tempTable.GetColumnName();
                //输出行
                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.getStatus().msg="删除数据成功";
                break;
            case "update":
                //TODO
                //主键更改问题
                table = db.getTables().get(t.table_name);
                table.update(t.expression,t.conditions);
                //更新成功后，返回插入后的表情况
                //将表信息输出给client
                tempTable= db.getTables().get(t.table_name);
                //输出列
                resp.columnsList = tempTable.GetColumnName();
                //输出行
                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.getStatus().msg="更新数据成功";
                break;
            case "select":
                //先测试前面的部分
                break;


        }

        // TODO 根据数据库处理结果返回给客户端
        return resp;
    }

}
