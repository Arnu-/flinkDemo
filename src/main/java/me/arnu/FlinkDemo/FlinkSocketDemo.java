package me.arnu.FlinkDemo;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSocketDemo {
    private final static Logger logger = LoggerFactory.getLogger("FlinkSocketDemo");

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            port = 50190;
            System.err.println("没有指定port参数，使用默认值" + port);
        }
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port, "\n");
        //System.out.println(text);
        long acceptTime = System.currentTimeMillis();
        logger.info("--------------------------out1" + "连接端口时间：" + acceptTime);
        text.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");
    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            long outTime = System.currentTimeMillis();
            logger.info("--------------------------out3 输出时间" + outTime);
            return "(" +
                    word + "," + count
                    + ')';
        }
    }
}
