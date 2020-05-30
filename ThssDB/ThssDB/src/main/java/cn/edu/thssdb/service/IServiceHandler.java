package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.statement_data;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import cn.edu.thssdb.parser.SQLLexer;
import java.util.Date;
import cn.edu.thssdb.parser.mySQLvisitor;
import cn.edu.thssdb.server.ThssDB;

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
    public DisconnetResp disconnect(DisconnetResp req) throws TException {
        // TODO
        // 疑惑？
        DisconnetResp resp = new DisconnetResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {

        long session =req.sessionId;
        CharStream input = CharStreams.fromString(req.statement);
        SQLLexer lexer = new SQLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseTree tree = parser.sql_stmt_list(); // parse
        mySQLvisitor visitor =new mySQLvisitor();
        statement_data t = visitor.visit(tree);
        System.out.print("收到信息，类型为:");
        System.out.print(t.kind);
        //在此处语法解析完成，并生成 statement_data t，请对t进行访问，以修改数据库
        // TODO 处理数据库


        // TODO 根据数据库处理结果返回给客户端
        ExecuteStatementResp resp = new ExecuteStatementResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }
}
