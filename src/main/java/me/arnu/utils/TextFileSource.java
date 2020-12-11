package me.arnu.utils;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class TextFileSource extends RichSourceFunction<String> {
    private volatile Boolean isRunning = true;

    private volatile String fileName;
    private volatile long sleep;
    private volatile long batch;
    private volatile int showStep;

    /**
     * 读取文件并逐行发送给处理环境的源
     *
     * @param fileName 文件名
     * @param sleep    一个批次的睡眠时间
     * @param batch    一个批次的发送行数
     * @param showStep 显示进度的步进
     */
    public TextFileSource(String fileName, long sleep, long batch, int showStep) {
        this.fileName = fileName;
        this.sleep = sleep;
        this.batch = batch;
        this.showStep = showStep;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        Scanner inputStream = null;
        try {
            inputStream = new Scanner(new FileInputStream(this.fileName));
        } catch (FileNotFoundException e) {
            System.out.println("File " + this.fileName + " was no found");
            System.exit(0);
        }
        String line = null;
        long count = 0;
        long to = 0;
        long oneBatch = 0;
        while (inputStream.hasNextLine()) {
            count++;
            if (showStep > 0) {
                long now = count / showStep;
                if (now > to) {
                    to = now;
                    System.out.print(count + ",");
                }
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
        System.out.print(count);
        inputStream.close();
        System.out.println("file length: " +count + ". file End.");
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
