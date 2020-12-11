package me.arnu.FlinkDemo;

import me.arnu.utils.ArnuSign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class SimpleSocketClient {
    private final static Logger logger = LoggerFactory.getLogger(SimpleSocketClient.class);
    private static Socket socket;
    private static PrintWriter printWriter;
    private static String host;
    private static int port;

    public static void main(String[] args) throws IOException {
        try {
            host = "localhost";
            port = 50190;
            System.out.println(ArnuSign.GetArnuSign() +
                    "----------------客户端执行，開始发送消息----------------");
            logger.info("host:" + host + ", port:" + port);
            //发送到8888端口
            init();
            Scanner scanner = new Scanner(System.in);
            while (true) {
                String msg = scanner.nextLine();
                SendMsg(msg);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null) {
                printWriter.close();
                printWriter = null;
            }
            if (socket != null && socket.isConnected()) {
                socket.close();
                socket = null;
            }
        }
    }

    private static void SendMsg(String msg) {
//        printWriter.write(msg);
        printWriter.println(msg);
        printWriter.flush();
    }

    private static void init() throws IOException {
        if (socket == null) {
            socket = new Socket(host, port);
        }
        if (printWriter == null) {
            OutputStream outputStream = socket.getOutputStream();
            printWriter = new PrintWriter(outputStream);
        }
    }
}
