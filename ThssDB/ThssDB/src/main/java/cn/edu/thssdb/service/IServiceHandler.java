package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import cn.edu.thssdb.parser.SQLLexer;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import cn.edu.thssdb.server.ThssDB;

public class IServiceHandler implements IService.Iface {
    public ThssDB server;

    public void pasteFolder(String srcFolder, String desFolder) {
        File linkFile = new File(Global.root + srcFolder);
        String[] fileNames = linkFile.list();
        File[] files = linkFile.listFiles();
        File targetFile = new File(Global.root + desFolder);
        if(!targetFile.exists()) {
            targetFile.mkdirs();
        }
        try {
            for (int i = 0; i < fileNames.length; i++) {
                if(files[i].isDirectory()) {
                    pasteFolder(srcFolder+"/"+fileNames[i], desFolder+"/"+fileNames[i]);
                }
                else {
                    FileReader fileReader = new FileReader(Global.root + srcFolder + "/" + fileNames[i]);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    File savedFile = new File(Global.root + desFolder + "/" + fileNames[i]);
                    if(!savedFile.exists()) {
                        savedFile.createNewFile();
                    }
                    FileWriter fileWriter = new FileWriter(Global.root + desFolder + "/" + fileNames[i],true);
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        // line写入文件夹中
                        bufferedWriter.write(line + '\n');
                    }
                    bufferedWriter.flush();
                    bufferedWriter.close();
                    fileReader.close();
                    bufferedReader.close();
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void clearFolder(String targetFolder) {
        File file = new File(Global.root + targetFolder);
        if(file.isFile()) {
            file.delete();
        }
        else {
            File[] files = file.listFiles();
            if(files == null) {
                file.delete();
            }
            else {
                for(int i=0; i<files.length; i++) {
                    clearFolder(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }

    public void saveConfig(boolean AUTO_COMMIT, boolean TRANSACTION, int COMMIT_VERSION, long session) {
        try {
            FileWriter confWriter = new FileWriter(Global.root + "/config/" + session + ".txt", false);
            BufferedWriter conbWriter = new BufferedWriter(confWriter);
            conbWriter.write(AUTO_COMMIT +"\n" + TRANSACTION + "\n" + COMMIT_VERSION);
            conbWriter.flush();
            conbWriter.close();
            confWriter.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public ExecuteStatementResp sqlHandler(String sql, long session) throws Exception {
        try {
            ExecuteStatementReq req = new ExecuteStatementReq();
            req.statement = sql;
            req.sessionId = session;
            return executeStatement(req);
        } catch(Exception e) {
            throw e;
        }
    }


    @Override
    public GetTimeResp getTime(GetTimeReq req) {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) {

        // TODO
        long sessionId = server.new_session();
        System.out.println(req.username);
        System.out.println(req.password);
        ConnectResp resp = new ConnectResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        resp.setSessionId(sessionId);
        System.out.println(resp.sessionId);

        return resp;
    }

    @Override
    public DisconnectResp disconnect(DisconnectReq req) {
        // TODO
        DisconnectResp resp = new DisconnectResp();
        server.remove_session(req.sessionId);
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        resp.status.msg = "成功断开连接，欢迎再次使用";

        //写文件
        ThssDB.manager.quit();

        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) {

        Table tempTable, table;
        boolean success;
        int retCase;

        // 准备用于返回的数据结构
        ExecuteStatementResp resp = new ExecuteStatementResp();
        resp.setStatus(new Status(Global.FAILURE_CODE));
        // 作为flag的数据结构
        boolean AUTO_COMMIT = true;
        boolean TRANSACTION = false;
        int COMMIT_VERSION = 0;
        // 为了多开而准备的文件夹结构
        long session = req.sessionId;
        File saveFolder = new File(Global.root + "/save/" + session);
        File logFile = new File(Global.root + "/log/" + session + ".txt");
        File configFile = new File(Global.root + "/config/" + session + ".txt");
        try {
            if (!saveFolder.exists())
                saveFolder.mkdirs();
            if (!logFile.exists()){
                File logFolder = new File(Global.root + "/log");
                logFolder.mkdirs();
                logFile.createNewFile();
            }

            if (!configFile.exists()) {
                File configFolder = new File(Global.root + "/config");
                configFolder.mkdirs();
                configFile.createNewFile();
                // 初始化session的config文件
                FileWriter initfWriter = new FileWriter(Global.root + "/config/" + session + ".txt", false);
                BufferedWriter initbWriter = new BufferedWriter(initfWriter);
                initbWriter.write("true\nfalse\n0");
                initbWriter.flush();
                initbWriter.close();
                initfWriter.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 从config中读出auto_commit, transaction, commit_version
        try {
            FileReader initfReader = new FileReader(Global.root + "/config/" + session + ".txt");
            BufferedReader initbReader = new BufferedReader(initfReader);
            AUTO_COMMIT = initbReader.readLine().equals("true");
            TRANSACTION = initbReader.readLine().equals("true");
            COMMIT_VERSION = Integer.parseInt(initbReader.readLine());
            initfReader.close();
            initbReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 获取输入以在对应sessionId下进行特殊处理
        // 转成小写以规避大小写问题
        String tempstring = req.statement.toLowerCase();
        if (tempstring.toCharArray()[tempstring.length()-1]==';'){
            tempstring=tempstring.substring(0,tempstring.length()-1);
        }
        CharStream input = CharStreams.fromString(tempstring);
        // 特殊情况：如果TRANSACTION = true，除非操作是commit或rollback或end transaction，全部写入log/session.txt并返回
        if(TRANSACTION) {
            String command = input.toString();
            if((!command.equals("commit")) && (!command.equals("rollback")) && (!command.equals("end transaction"))) {
                try {
                    FileWriter logfWriter = new FileWriter(Global.root + "/log/" + session + ".txt", true);
                    BufferedWriter logbWriter = new BufferedWriter(logfWriter);
                    logbWriter.write(command + "\n");
                    logbWriter.flush();
                    logbWriter.close();
                    logfWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                resp.status.code = Global.SUCCESS_CODE;
                resp.status.msg = "已将指令暂存。完成事务所有指令的输入后，请手动commit";
                return resp;
            }
        }
        // 特殊input的判断与分析
        // set auto commit true
        if (input.toString().equals("set auto commit true")) {
            if (!AUTO_COMMIT) {
                AUTO_COMMIT = true;
                TRANSACTION = false;
                saveConfig(AUTO_COMMIT, TRANSACTION, COMMIT_VERSION, session);
                resp.status.code = Global.SUCCESS_CODE;
                resp.status.msg = "设置为auto commit模式";
            } else {
                resp.status.msg = "多余的操作：已处于auto commit模式";
            }
            return resp;
        }
        // set auto commit false
        else if (input.toString().equals("set auto commit false")) {
            if (AUTO_COMMIT) {
                AUTO_COMMIT = false;
                saveConfig(AUTO_COMMIT, TRANSACTION, COMMIT_VERSION, session);
                resp.status.code = Global.SUCCESS_CODE;
                resp.status.msg = "退出auto commit模式";
            }
            else {
                resp.status.msg = "多余的操作：已退出auto commit模式";
            }
            return resp;
        }
        // begin transaction
        else if(input.toString().equals("begin transaction")) {
            if(AUTO_COMMIT) {
                resp.status.msg = "无效的操作：处于auto commit模式";
            }
            else {
                resp.status.code = Global.SUCCESS_CODE;
                resp.status.msg = "启动事务（输入操作指令完成后，请手动输入commit指令）";
                // 将当前数据库的表存入save/session/commit0下
                pasteFolder("/data", "/save/"+session+"/commit"+COMMIT_VERSION);
                TRANSACTION = true;
                COMMIT_VERSION += 1;
                saveConfig(AUTO_COMMIT, TRANSACTION, COMMIT_VERSION, session);
            }
            return resp;
        }
        // commit
        else if(input.toString().equals("commit")) {
            if(AUTO_COMMIT) {
                resp.status.msg ="多余的操作：已处于auto commit模式，无需手动commit";
            }
            else {
                try {
                    // 读取log/session.txt中的指令，逐个执行
                    FileReader logfReader = new FileReader(Global.root+"/log/"+session+".txt");
                    BufferedReader logbReader = new BufferedReader(logfReader);
                    String sql = null;
                    ArrayList<String> sqls = new ArrayList<>();
                    while((sql = logbReader.readLine()) != null) {
                        sqls.add(sql);
                    }
                    logfReader.close();
                    logbReader.close();
                    if(sqls.size() == 0) {
                        resp.status.msg = "无效的操作：没有暂存的指令";
                        return resp;
                    }
                    else {
                        // 为了执行操作，暂时放开flag
                        AUTO_COMMIT = true;
                        TRANSACTION = false;
                        saveConfig(AUTO_COMMIT, TRANSACTION, COMMIT_VERSION, session);
                        // 逐条获取指令，调用函数运行
                        String[] sqlsList = (String[])sqls.toArray(new String[sqls.size()]);
                        for(int i=0; i<sqlsList.length; i++) {
                            try {
                                ExecuteStatementResp single = sqlHandler(sqlsList[i], session);
                                if(single.status.code == Global.FAILURE_CODE) {
                                    // rollback
                                    pasteFolder("/save/"+session+"/commit"+(COMMIT_VERSION-1), "/data");
                                    resp.status.msg = "暂存指令第"+i+"条运行失败：\n"+single.status.msg+"\n自动回滚至第"+COMMIT_VERSION+"次提交的版本";
                                    return resp;
                                }
                            } catch(Exception e) {
                                // rollback
                                pasteFolder("/save/"+session+"/commit"+(COMMIT_VERSION-1), "/data");
                                resp.status.msg = "暂存指令第"+i+"条运行出现异常，自动回滚至第"+(COMMIT_VERSION-1)+"次提交的版本";
                                return resp;
                            }
                        }
                        // 遍历结束后，所有log都被执行，commit完成，保存新的save并更新COMMIT_VERSION
                        pasteFolder("/data", "/save/"+session+"/commit"+COMMIT_VERSION);
                        AUTO_COMMIT = false;
                        TRANSACTION = true;
                        COMMIT_VERSION += 1;
                        saveConfig(AUTO_COMMIT, TRANSACTION, COMMIT_VERSION, session);
                        resp.status.code = Global.SUCCESS_CODE;
                        resp.status.msg = "所有暂存指令已成功提交！";
                        return resp;
                    }
                } catch(Exception e) {
                    pasteFolder("/save/"+session+"/commit"+(COMMIT_VERSION-1), "/data");
                    e.printStackTrace();
                } finally {
                    // 清空log
                    try {
                        FileWriter clearfWriter = new FileWriter(Global.root + "/log/" + session + ".txt", false);
                        BufferedWriter clearbWriter = new BufferedWriter(clearfWriter);
                        clearbWriter.write("");
                        clearbWriter.flush();
                        clearbWriter.close();
                        clearfWriter.close();
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return resp;
        }
        // rollback
        else if(input.toString().equals("rollback")) {
            // 清空log
            try {
                FileWriter clearfWriter = new FileWriter(Global.root + "/log/" + session + ".txt", false);
                BufferedWriter clearbWriter = new BufferedWriter(clearfWriter);
                clearbWriter.write("");
                clearbWriter.flush();
                clearbWriter.close();
                clearfWriter.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
            // 回退到上一次（VERSION-1）次commit并删除该次commit
            if(!TRANSACTION) {
                resp.status.msg = "无效的指令：尚未处在一个事务中";
            }
            else if(COMMIT_VERSION == 0) {
                resp.status.msg = "无效的指令：已经是最初始的版本了";
            }
            else {
                // 将最近的一次commit：save/commitX中的数据回滚
                COMMIT_VERSION -= 1;
                pasteFolder("/save/"+session+"/commit"+ COMMIT_VERSION, "/data");
                clearFolder("/save/"+session+"/commit"+ COMMIT_VERSION);
                resp.status.msg = "成功回滚至第"+COMMIT_VERSION+"次commit的版本";
                resp.status.code = Global.SUCCESS_CODE;
            }
            return resp;
        }
        // end transaction
        else if(input.toString().equals("end transaction")) {
            if(!TRANSACTION) {
                resp.status.msg = "无效的操作：尚未处在一个事务中";
            }
            else {
                TRANSACTION = false;
                saveConfig(AUTO_COMMIT, TRANSACTION, COMMIT_VERSION, session);
                resp.status.code = Global.SUCCESS_CODE;
                resp.status.msg = "成功关闭本事务";
            }
            return resp;
        }
        //
//        else if(input.toString().equals("clear")) {
//            clearFolder("/data/saved_data/saved_databases");
//        }
//
        SQLLexer lexer = new SQLLexer(input);   // input:client输入的内容
        //此处截取输出日志
        PrintStream oldPrintStream = System.err; //将原来的System.out交给printStream 对象保存
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setErr(new PrintStream(bos));//设置新的out
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseTree tree = parser.sql_stmt_list(); // parse
        mySQLvisitor visitor = new mySQLvisitor();
        statement_data t = visitor.visit(tree);
        if (t==null){
            //语法错误
            resp.status.msg =bos.toString();

            System.setErr(oldPrintStream);//恢复原来的System.out
            return resp;
        }
        System.setErr(oldPrintStream);//恢复原来的System.out
        //在此处语法解析完成，并生成 statement_data t，请对t进行访问，以修改数据库
        // TODO 处理数据库
        // 忽略异常处理

        Database db = ThssDB.manager.getWorkingDb();
        switch (t.kind){
            case "show_table": {
                //展示数据
                //将表信息输出给client
                tempTable = db.getTables().get(t.table.table_name);
                if (tempTable==null){
                    resp.status.code = Global.FAILURE_CODE;
                    resp.getStatus().msg = "此表不存在";
                    return resp;
                }
//                //输出列
//                resp.columnsList = tempTable.GetColumnName();
//                //输出行
//                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.status.code=Global.SUCCESS_CODE;
                StringBuilder msg = new StringBuilder();
                for(int i=0; i<tempTable.columns.size();i++){
                    String tempmsg = tempTable.columns.get(i).toString();
                    msg.append(tempmsg);
                    msg.append('\n');
                }
                resp.getStatus().msg =msg.toString();
                break;
            }
            case "use_database": {
                //切换数据库
                //已测试
                success = ThssDB.manager.switchDatabase(t.database_name);
                if (success) {
                    resp.status.code = Global.SUCCESS_CODE;
                    resp.getStatus().msg = "切换数据库成功";
                } else {
                    resp.status.msg = "该数据库不存在";
                }

                //异常状况:1.切换的数据库不存在
                break;
            }
            case "create_database": {
                //已测试
                success = Manager.createDatabaseIfNotExists(t.database_name);
                if (success) {
                    resp.status.code = Global.SUCCESS_CODE;
                    resp.getStatus().msg = "创建数据库成功";
                }
                else {
                    resp.status.msg = "创建数据库失败:已存在同名数据库";
                }
                //异常状况:1.试图创建的数据库已存在
                break;
            }
            case "drop_database": {
                //已测试
                retCase = Manager.deleteDatabase(t.database_name);
                switch (retCase) {
                    case 0:
                        resp.status.code = Global.SUCCESS_CODE;
                        resp.getStatus().msg = "删除数据库成功";
                        break;
                    case 1:
                        resp.getStatus().msg = "删除数据库失败:数据库不存在";
                        break;
                    case 2:
                        resp.getStatus().msg = "删除数据库失败:数据库非空";
                        break;
                    case 3:
                        resp.getStatus().msg = "删除数据库失败:请勿删除默认数据库";
                        break;
                    default:
                        break;
                }
                //异常状况:1.数据库不存在
                //        2.数据库非空
                break;
            }
            case "create_table": {
                //已测试
                success = db.create(t.table.table_name, t.getColumns());
                if (success) {
                    resp.status.code = Global.SUCCESS_CODE;
                    resp.getStatus().msg = "创建表成功";
                } else {
                    resp.status.code = Global.FAILURE_CODE;
                    resp.status.msg = "创建表失败:已存在同名表";
                }
                //异常状况:1.试图创建的表已存在
                break;
            }
            case "drop_table": {
                success = db.drop(t.table.table_name);
                if (success) {
                    resp.status.code = Global.SUCCESS_CODE;
                    resp.getStatus().msg = "删除表成功";
                }
                else {
                    resp.status.msg = "删除表失败:不存在这张表";
                }
                //异常状况:1.试图删除的表不存在
                break;
            }
            case "insert": {
                //已测试
                //错误情况尚未处理(多主键相同)
                //记得处理不存在这张表的情况
                table = db.getTables().get(t.table_name);
                if (table==null){
                    resp.status.code = Global.FAILURE_CODE;
                    resp.getStatus().msg = "此表不存在";
                    return resp;
                }
                Entry[] temp_list = new Entry[table.columns.size()];
                //找每个名字对应的属性
                if (t.column_names.size() == 0) {
                    // TODO：可空问题
                    for (int j = 0; j < table.columns.size(); j++) {
                        if (t.value_entrys.get(j) != null) {
                            temp_list[j] = Database.GetEntry(table.columns.get(j).getType(), t.value_entrys.get(j));
                        }
                    }
                } else {
                    for (int i = 0; i < t.column_names.size(); i++) {
                        //其中可以搞点异常处理
                        for (int j = 0; j < table.columns.size(); j++) {
                            if (t.column_names.get(i).equals(table.columns.get(j).getName())) {
                                //第j个属性值应为第i个属性
                                //TODO：增加前后数量不匹配的return
                                temp_list[j] = Database.GetEntry(table.columns.get(j).getType(), t.value_entrys.get(i));

                            }
                        }
                    }
                }
                //然后看看有没有可空属性空
                boolean bigbreak = false;
                for (int i = 0; i < table.columns.size(); i++) {
                    if (temp_list[i] == null && table.columns.get(i).isNotNull()) {
                        //不可空可空，报错
                        resp.getStatus().msg = "插入数据失败,原因:不可空数据缺失";
                        bigbreak = true;
                        break;
                    }
                    if (!table.columns.get(i).isNotNull()&&temp_list[i] == null){
                        temp_list[i] = new Entry("null");
                    }
                }
                if (bigbreak){
                    break;
                }
                try {
                    table.insert(temp_list);
                } catch (cn.edu.thssdb.exception.DuplicateKeyException e) {
                    resp.getStatus().msg = "已存在相同的主键";
                    break;
                }
                //插入成功后，返回插入后的表情况
                //将表信息输出给client
//                tempTable = db.getTables().get(t.table_name);
//                //输出列
//                resp.columnsList = tempTable.GetColumnName();
//                //输出行
//                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.status.code=Global.SUCCESS_CODE;
                resp.getStatus().msg = "插入数据成功";
                break;
            }
            case "delete": {
                //记得处理不存在这张表的情况
                table = db.getTables().get(t.table_name);
                if (table==null){
                    resp.status.code = Global.FAILURE_CODE;
                    resp.getStatus().msg = "此表不存在";
                    return resp;
                }
                table.delete(t.conditions);
                //删除成功后，返回插入后的表情况
                //将表信息输出给client
                tempTable = db.getTables().get(t.table_name);
                //输出列
                resp.columnsList = tempTable.GetColumnName();
                //输出行
                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.getStatus().msg = "删除数据成功";
                resp.status.code=Global.SUCCESS_CODE;
                break;
            }
            case "update": {
                //记得处理不存在这张表的情况
                //TODO
                //主键更改问题
                table = db.getTables().get(t.table_name);
                if (table==null){
                    resp.status.code = Global.FAILURE_CODE;
                    resp.getStatus().msg = "此表不存在";
                    return resp;
                }
                table.update(t.expression, t.conditions);
                //更新成功后，返回插入后的表情况
                //将表信息输出给client
                tempTable = db.getTables().get(t.table_name);
                //输出列
                resp.columnsList = tempTable.GetColumnName();
                //输出行
                resp.rowList = Database.BTreeParseLLS(tempTable.index);
                resp.getStatus().msg = "更新数据成功";
                resp.status.code=Global.SUCCESS_CODE;
                break;
            }
            case "select": {
                //先测试前面的部分
                QueryResult result = db.select(t.table_names, t.equalexpressions, t.conditions);
                if (result==null){
                    resp.status.code = Global.FAILURE_CODE;
                    resp.getStatus().msg = "此表不存在";
                    return resp;
                }
                //输出列(暂时输出所有)
                resp.columnsList = selectAttr(result.columnName,t.FullColumns);
                List<List<String>> tempStrList = new ArrayList<>();
                //输出行
                for (int i = 0; i < result.entry.size(); i++) {
                    List<String> tempStr = new ArrayList<>();
                    List<Entry> tempEntry = result.entry.get(i);
                    for (int j=0;j<tempEntry.size();j++){
                        tempStr.add(tempEntry.get(j).toString());
                    }
                    tempStrList.add(tempStr);
                }
                resp.rowList = select(tempStrList,result.columnName,t.FullColumns);
                resp.getStatus().code = Global.SUCCESS_CODE;
                resp.getStatus().msg = "select成功";
                break;
            }
        }
        return resp;
    }

    //拿到需要的select列
    public static List<List<String>> select(List<List<String>> tempStrList,List<String>now,List<FullColumn>shouldSelect){
        List<List<String>> ret = new ArrayList<>();
        for (List<String> strings : tempStrList) {
            List<String> tempStr = new ArrayList<>();
            for (int j = 0; j < now.size(); j++) {
                for (FullColumn str : shouldSelect) {
                    String str_a = str.tableName + ".";
                    if (str_a.equals("NULL.")) {
                        str_a = "";
                    }
                    String tempstr2 = str_a + str.column_name;
                    if (tempstr2.equals(now.get(j))) {
                        tempStr.add(strings.get(j));
                    }
                }
            }
            ret.add(tempStr);
        }
        return ret;
    }
    public static List<String> selectAttr(List<String>now,List<FullColumn>shouldSelect){
        List<String> ret = new ArrayList<>();
        for (String str:now){
            for (FullColumn fullColumn : shouldSelect) {
                String str_a = fullColumn.tableName + ".";
                if (str_a.equals("NULL.")) {
                    str_a = "";
                }
                String tempstr2 = str_a + fullColumn.column_name;
                if (tempstr2.equals(str)) {
                    ret.add(str);
                    break;
                }
            }

        }
        return ret;
    }

}
