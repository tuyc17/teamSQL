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
            IServiceHandler t = new IServiceHandler();
            ExecuteStatementReq test = new ExecuteStatementReq();
            test.statement = "123";
            t.executeStatement(test);
            //测试结束
            server.serve();
        } catch (TTransportException e) {
            logger.error(e.getMessage());
        }
    }

    private static class ThssDBHolder {
        private static final ThssDB INSTANCE = new ThssDB();

        private ThssDBHolder() {

        }
    }
}
