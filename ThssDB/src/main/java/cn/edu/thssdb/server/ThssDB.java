package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThssDB {

    private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

    private static IServiceHandler handler;
    private static IService.Processor processor;
    private static TServerSocket transport;
    private static TServer server;

    private Manager manager;

    public static ThssDB getInstance() {
        return ThssDBHolder.INSTANCE;
    }

    public static void main(String[] args) {
        ThssDB server = ThssDB.getInstance();
        server.start();
    }

    private void start() {
        handler = new IServiceHandler();
        processor = new IService.Processor(handler);
        Runnable setup = () -> {
            try {
                setUp(processor);
            } catch (TException e) {
                e.printStackTrace();
            }
        };
        new Thread(setup).start();
    }

    private static void setUp(IService.Processor processor) throws TException {
        try {
            transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
            server = new TSimpleServer(new TServer.Args(transport).processor(processor));
            logger.info("Starting ThssDB ...");

            //此处用于测试
//            IServiceHandler t = new IServiceHandler();
//            ExecuteStatementReq test = new ExecuteStatementReq();
//          test.statement = "CREATE TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID))";
//          //CREATE测试完成
//          test.statement ="INSERT INTO person VALUES ('Bob', 15)";
//          //insert测试完成
//          test.statement ="DELETE FROM tableName WHERE attrName = attValue";
//          delete测试完成
//          test.statement ="UPDATE  tableName  SET  attrName = attrValue  WHERE  attrName = attrValue";
//          test.statement ="UPDATE  tableName  SET  attrName = attrValue  ";
//          //update测试完成
//          test.statement ="SELECT tableName1.AttrName1, tableName1.AttrName2, tableName2.AttrName1, tableName2.AttrName2  " +
//                    "FROM  tableName1 JOIN tableName2  ON  tableName1.attrName1 = tableName2.attrName2  " +
//                    "WHERE  attrName1 = attrValue ";
//            t.executeStatement(test);
            //测试结束
            server.serve();
        } catch (TTransportException e) {
            logger.error(e.getMessage());
        }
    }

    private static class ThssDBHolder {
        private static final ThssDB INSTANCE = new ThssDB();

        private ThssDBHolder() {
            ;

        }
    }
}
