package me.arnu.Socket;

import me.arnu.utils.ArnuSign;
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
//        fileName = "D:\\code\\1\\flinksql\\src\\main\\resources\\flink.txt";
        fileName = "D:\\tmp\\20201211\\jdxm_cctv5.20201211.log";
        port = 50190;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println(ArnuSign.GetArnuSign() +
                "----------------服务端执行，開始监听请求----------------");
        try {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();//開始监听
                    logger.info("有请求接入，" + socket.getRemoteSocketAddress());
                    init(socket);

//                    sendFile(socket);
//                    System.in.read();

                    Scanner scanner = new Scanner(System.in);
                    while (true) {
                        String msg = scanner.nextLine();
                        SendMsg(msg);
                        socket.sendUrgentData(0);
                        Thread.sleep(300);
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

    private static void sendFile(Socket socket) throws InterruptedException, IOException {
        Scanner inputStream = null;
        try {
            inputStream = new Scanner(new FileInputStream(fileName));

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
                    socket.sendUrgentData(0);
                    oneBatch = 0;
                }
            }
            inputStream.close();
            System.out.println("file End.");
        } catch (FileNotFoundException e) {
            System.out.println("File " + fileName + " was no found");
            System.exit(0);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}
