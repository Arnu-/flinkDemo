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

    public TextFileSource(String fileName, String sleep, String batch, String showStep) {
        this.fileName = fileName;
        this.sleep = Long.parseLong(sleep);
        this.batch = Long.parseLong(batch);
        this.showStep = Integer.parseInt(showStep);
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
        System.out.println("file End.");
        for(int i = 0; i< 20; i++)
        {
            TimeUnit.MILLISECONDS.sleep(this.sleep);
            ctx.collect("the end.");
            System.out.println("the end.");
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
