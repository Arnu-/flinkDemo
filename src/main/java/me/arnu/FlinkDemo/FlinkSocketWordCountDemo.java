package me.arnu.FlinkDemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkSocketWordCountDemo {
    private final static Logger logger = LoggerFactory.getLogger("FlinkSocketDemo");

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("没有指定port参数，使用默认值9000");
            port = 50190;
        }
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port, "\n");
        //System.out.println(text);
        long acceptTime = System.currentTimeMillis();
        logger.info("--------------------------out1" + "连接端口时间：" + acceptTime);
        // t1，接收到的时间
        //计算数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            int cishu = 0;

            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                logger.info("---------------------------------------out2 处理时间" + "当前读入次数第" + cishu + "次");
                String[] splits = value.split("\\s");
                cishu++;
                //long handleTime=System.currentTimeMillis();
                //System.out.println("---------------------------------------out2 处理时间" + "当前读入次数第" + cishu + "次");
                //int i =0;
                for (String w : splits) {

                    logger.info(w + "开始处理数据时间" + System.currentTimeMillis());
                    out.collect(new WordWithCount(w, 1));
                }
                //cishu++;
            }
        })//打平操作，把每行的单词转为<word,count>类型的数据
                //针对相同的word数据进行分组
                .keyBy("word")
                //指定计算数据的窗口大小和滑动窗口大小
                .timeWindow(Time.seconds(5))
                .sum("count");
        //System.out.println("处理结束时间out"+ System.currentTimeMillis());
        //把数据打印到控制台,使用一个并行度
        windowCount.print().setParallelism(1);


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
