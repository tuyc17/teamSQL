package cn.edu.thssdb.client;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.table.DefaultTableModel;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import javax.swing.JTable;
import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.MessageFormat;
import java.util.Random;


public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    static final String HOST_ARGS = "h";
    static final String HOST_NAME = "host";

    static final String HELP_ARGS = "help";
    static final String HELP_NAME = "help";

    static final String PORT_ARGS = "p";
    static final String PORT_NAME = "port";

    private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
    private static final Scanner SCANNER = new Scanner(System.in);

    private static TTransport transport;
    private static TProtocol protocol;
    private static IService.Client client;
    private static CommandLine commandLine;
    public static long session = -1;

    public static void main(String[] args) {
        commandLine = parseCmd(args);
        if (commandLine.hasOption(HELP_ARGS)) {
            showHelp();
            return;
        }
        try {
            echoStarting();
            String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
            int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
            transport = new TSocket(host, port);
            transport.open();
            protocol = new TBinaryProtocol(transport);
            client = new IService.Client(protocol);
            boolean open = true;
            //不需要登录
            sendconnect("test_username", "test_password");
            while (true) {
                print(Global.CLI_PREFIX);
                String msg = SCANNER.nextLine();
                String[] msglist = msg.trim().split(" ");
                long startTime = System.currentTimeMillis();
                switch (msglist[0]) {
                    case Global.CONNECT:
                        // 目前暂时没有密码校验
                        if (msglist.length < 3) {
                            sendconnect("msglist[1]", "msglist[2]");
                        } else {
                            sendconnect(msglist[1], msglist[2]);
                        }
                        break;
                    case Global.SHOW_TIME:
                        getTime();
                        break;
                    case Global.QUIT:
                        sendDisconnect(session);
                        session = -1;
                        open = false;
                        break;
                    default:
                        // 应该是服务端告诉客户端这个语句不通顺，客户端没有语法解析，应该是不知道的
                        // 故default应该是语法执行，客户端等回信，而不是输出invalid statements
                        if (session == -1) {
                            println("尚未连接！");
                        } else {
                            send_statment(session, msg.trim());
                        }
                        break;
                }
                long endTime = System.currentTimeMillis();
                println("It costs " + (endTime - startTime) + " ms.");
                if (!open) {
                    break;
                }
            }
            transport.close();
        } catch (TTransportException e) {
            logger.error(e.getMessage());
        }
    }

    private static void getTime() {
        GetTimeReq req = new GetTimeReq();
        try {
            println(client.getTime(req).getTime());
        } catch (TException e) {
            logger.error(e.getMessage());
        }
    }

    private static void sendconnect(String username, String password) {
        ConnectReq req = new ConnectReq(username, password);
        try {
            ConnectResp resp = client.connect(req);
            Status temp = resp.getStatus();
            //异常处理省略,理论上应该通过这个观察是否异常
            session = resp.getSessionId();
            System.out.println(session);
        } catch (TException e) {
            logger.error(e.getMessage());
        }
    }
    private static void sendDisconnect(long session) {
        DisconnectReq req = new DisconnectReq(session);
        try {
            DisconnectResp resp = client.disconnect(req);
            Status temp = resp.getStatus();
            //异常处理省略,理论上应该通过这个观察是否异常
            println(temp.msg);
        } catch (TException e) {
            logger.error(e.getMessage());
        }
    }


    private static void send_statment(long sessionId, String statment) {
        ExecuteStatementReq req = new ExecuteStatementReq(sessionId, statment);
        try {
            ExecuteStatementResp resp = client.executeStatement(req);
            Status temp = resp.getStatus();
            if (temp.code == Global.SUCCESS_CODE) {
                System.out.println("语句执行成功");
            } else if (temp.code == Global.FAILURE_CODE) {
                System.out.println("语句执行失败");
            }
            System.out.print("服务端消息:");
            System.out.println(temp.msg);
            if (resp.columnsList != null && resp.rowList != null) {
                Object[][] templist =new Object[resp.rowList.size()][resp.columnsList.size()];
                for (int i=0;i<resp.rowList.size();i++){
                    templist[i] = resp.rowList.get(i).toArray();
                }
                JTable temptable = new JTable(templist,resp.columnsList.toArray());
                JFrame tempFrame = new JFrame();

                //tempFrame.add(addBtn, BorderLayout.NORTH);
                tempFrame.add(new JScrollPane(temptable));
                tempFrame.setSize(400, 300);
                tempFrame.setLocationRelativeTo(null);
                tempFrame.setVisible(true);
            }
//            if (resp.rowList != null) {
//                System.out.println(resp.rowList.toString());
//            }
            System.out.println("");
        } catch (TException e) {
            logger.error(e.getMessage());
        }
    }

    static Options createOptions() {
        Options options = new Options();
        options.addOption(Option.builder(HELP_ARGS)
                .argName(HELP_NAME)
                .desc("Display help information(optional)")
                .hasArg(false)
                .required(false)
                .build()
        );
        options.addOption(Option.builder(HOST_ARGS)
                .argName(HOST_NAME)
                .desc("Host (optional, default 127.0.0.1)")
                .hasArg(false)
                .required(false)
                .build()
        );
        options.addOption(Option.builder(PORT_ARGS)
                .argName(PORT_NAME)
                .desc("Port (optional, default 6667)")
                .hasArg(false)
                .required(false)
                .build()
        );
        return options;
    }

    static CommandLine parseCmd(String[] args) {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            println("Invalid command line argument!");
            System.exit(-1);
        }
        return cmd;
    }

    static void showHelp() {
        // TODO
        println("DO IT YOURSELF");
    }

    static void echoStarting() {
        println("----------------------");
        println("Starting ThssDB Client");
        println("----------------------");
    }

    static void print(String msg) {
        SCREEN_PRINTER.print(msg);
    }

    static void println() {
        SCREEN_PRINTER.println();
    }

    static void println(String msg) {
        SCREEN_PRINTER.println(msg);
    }
}
