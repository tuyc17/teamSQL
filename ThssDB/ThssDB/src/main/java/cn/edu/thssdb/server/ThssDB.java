package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ThssDB {

    private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

    private static IService.Processor processor;
    public List<Long> sessions = new ArrayList<>();

    public static Manager manager;

    public static ThssDB getInstance() {
        return ThssDBHolder.INSTANCE;
    }

    public long new_session(){
        Random r = new Random();
        long temp = 0;
        boolean temp_bool=true;
        while(temp_bool){
            temp = r.nextLong();
            temp_bool=false;
            for (Long session : sessions) {
                if (temp == session) {
                    temp_bool = true;
                    break;
                }
            }
        }
        sessions.add(temp);
        return temp;
    }

    public void remove_session(long session){
        for (int i = 0;i<sessions.size();i++){
            if (session==sessions.get(i)){
                sessions.remove(i);
                return;
            }
        }
    }

    public static void main(String[] args) {
        ThssDB server = ThssDB.getInstance();
        server.start();
    }

    public void start() {
        IServiceHandler handler = new IServiceHandler();
        handler.server=this;
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

    // 实例化manager
    private static void setUp(IService.Processor processor) throws TException {
        try {
            TServerSocket transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(processor));
            logger.info("Starting ThssDB ...");
            manager = Manager.getInstance();
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
