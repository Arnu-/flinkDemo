package me.arnu.flinksql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class SimpleSocketServer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleSocketServer.class);

    private static int port;
    private static PrintWriter printWriter;
    private static String fileName;
    private static int showStep;
    private static int batch;
    private static int sleep;

    public static void main(String[] args) throws IOException {
        showStep = 5;
        batch = 1;
        sleep = 100;
        fileName = "D:\\code\\1\\flinksql\\src\\main\\resources\\flink.txt";
        port = 50190;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("#     __\n" +
                "#    /  |  ____ ___  _  \n" +
                "#   / / | / __//   // / /\n" +
                "#  /_/`_|/_/  / /_//___/\n" +
                "----------------服务端执行，開始监听请求----------------");
        try {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();//開始监听
                    logger.info("有请求接入，" + socket.getRemoteSocketAddress());
                    init(socket);

//                    sendFile();
//                    System.in.read();

                    Scanner scanner = new Scanner(System.in);
                    while (true) {
                        String msg = scanner.nextLine();
                        SendMsg(msg);
                        socket.sendUrgentData(0);
                    }


//                    InputStream inputStream = socket.getInputStream();
//                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
//                    //获取请求内容
//                    String info;
//                    while ((info = bufferedReader.readLine()) != null) {
//                        System.out.println("我是server端，client请求为：" + info);
//                    }
//
//                    //关闭资源
//                    socket.shutdownInput();
//                    bufferedReader.close();
//                    inputStream.close();
//                    socket.close();
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        } finally {
            serverSocket.close();
        }

    }


    private static void SendMsg(String msg) {
//        printWriter.write(msg);
        printWriter.println(msg);
        printWriter.flush();
    }

    private static void init(Socket socket) throws IOException {
        if (printWriter == null) {
            OutputStream outputStream = socket.getOutputStream();
            printWriter = new PrintWriter(outputStream);
        }
    }

    private static void sendFile() throws InterruptedException {
        Scanner inputStream = null;
        try {
            inputStream = new Scanner(new FileInputStream(fileName));
        } catch (FileNotFoundException e) {
            System.out.println("File " + fileName + " was no found");
            System.exit(0);
        }
        String line = null;
        long count = 0;
        long to = 0;
        long oneBatch = 0;
        while (inputStream.hasNextLine()) {
            count++;
            long now = count / showStep;
            if (now > to) {
                to = now;
                System.out.print(count + ",");
            }
            line = inputStream.nextLine();
            SendMsg(line);
            oneBatch++;
            if (oneBatch >= batch) {
                if (sleep > 0) {
                    TimeUnit.MILLISECONDS.sleep(sleep);
                }
                oneBatch = 0;
            }
        }
        inputStream.close();
        System.out.println("file End.");
    }
}
