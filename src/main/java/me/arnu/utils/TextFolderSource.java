package me.arnu.utils;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class TextFolderSource extends RichSourceFunction<String> {
    private static final Logger logger = LoggerFactory.getLogger(TextFolderSource.class);
    private volatile Boolean isRunning = true;

    private volatile String folderName;
    private volatile long sleep;
    private volatile long batch;
    private volatile int showStep;

    public TextFolderSource(String folderName, String sleep, String batch, String showStep) {
        this.folderName = folderName;
        this.sleep = Long.parseLong(sleep);
        this.batch = Long.parseLong(batch);
        this.showStep = Integer.parseInt(showStep);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        List<String> files = new ArrayList<String>();
        File file = new File(this.folderName);
        File[] tempList = file.listFiles();

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                files.add(tempList[i].toString());
                //文件名，不包含路径
                //String fileName = tempList[i].getName();
            }
            if (tempList[i].isDirectory()) {
                //这里就不递归了，
            }
        }
        long count = 0;
//        int showStep = 100;
        long to = 0;
        long oneBatch = 0;
        for (String fileName : files) {
            logger.info( fileName + " file Start.");
            Scanner inputStream = null;
            try {
                inputStream = new Scanner(new FileInputStream(fileName));
            } catch (FileNotFoundException e) {
                logger.error("File " + fileName + " was no found");
                System.exit(0);
            }
            String line = null;
            while (inputStream.hasNextLine()) {
                count++;
                long now = count / showStep;
                if (now > to) {
                    to = now;
                    System.out.print(count + ",");
                }
                line = inputStream.nextLine();
                ctx.collect(line);
                oneBatch++;
                if (oneBatch >= this.batch) {
                    if (this.sleep > 0) {
                        TimeUnit.MILLISECONDS.sleep(this.sleep);
                    }
                    oneBatch = 0;
                }
            }
            inputStream.close();
            logger.info( fileName + " file End.");
        }
        logger.info("file all End.");

//        //BufferedReader bufferedReader =
//        new BufferedReader(new FileReader(this.fileName))
//                .lines().
//                collect(Collectors.toSet()). // You can also use list or any other Collection
//                forEach((line) -> {
//            try {
//                if (StringUtils.isBlank(line)) {
//                    ctx.collect(line);
//                }
//                TimeUnit.MILLISECONDS.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//        System.out.println("file End.");
//        while (isRunning) {
//            String line = bufferedReader.readLine();
//            if (StringUtils.isBlank(line)) {
//                continue;
//            }
//            ctx.collect(line);
//            TimeUnit.MILLISECONDS.sleep(100);
//        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
