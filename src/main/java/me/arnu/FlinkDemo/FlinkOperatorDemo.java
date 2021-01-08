package me.arnu.FlinkDemo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import me.arnu.utils.ArnuSign;
import me.arnu.utils.TextFileSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkOperatorDemo {
    private final static Logger logger = LoggerFactory.getLogger("FlinkOperatorDemo");

    public static void main(String[] args) throws Exception {
        ArnuSign.printSign();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String folderName = "D:\\dfjx\\20200210央视频\\20201203tab信息kafka上报\\cms_publish_data.log";
        int sleep = 300;
        int batch = 10;
        int showStep = 0;
        SingleOutputStreamOperator<String> source = env.addSource(new TextFileSource(folderName, sleep, batch, showStep));
        // 1、map, 用于输入一个参数产生一个参数，map的功能是对输入的参数进行转换操作。
        SingleOutputStreamOperator<JSONObject> jsonStream = source.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                try {
                    JSONObject jobj = JSONObject.parseObject(s);
                    return jobj;
                } catch (JSONException e) {
                    logger.error("转换失败：\n" + s, e);
                }
                return null;
            }
        }).name("将输入字符串变成json对象");

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream =
                jsonStream.flatMap(new FlatMapFunction<JSONObject, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(JSONObject jsonObject, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if (jsonObject == null) {
                    return;
                }
                JSONArray ZT_content = jsonObject.getJSONArray("ZT_content");
                if (ZT_content != null && ZT_content.size() > 0) {
                    for (Object o : ZT_content) {
                        JSONObject content = (JSONObject) o;
                        JSONArray ZT_leaf_video = content.getJSONArray("ZT_leaf_video");
                        if (ZT_leaf_video != null && ZT_leaf_video.size() > 0) {
                            for (Object v : ZT_leaf_video) {
                                JSONObject video = (JSONObject) v;
                                String ctype = video.getString("ctype");
                                if(ctype == null){
                                    logger.warn("有空类型数据：" + video.toJSONString());
                                }else {
                                    collector.collect(Tuple2.of(ctype, 1));
                                }
                            }
                        }
                    }
                }
            }
        }).name("拆分json格式平铺输出")

                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value.f0 != null && !value.f0.isEmpty();
                    }
                }).name("过滤指定的数据")


                .keyBy(0);

        keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + stringIntegerTuple2.f1);
            }
        }).name("分组后数据进行归并操作")

                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .maxBy(1)
                .print()
        ;

        env.execute("执行flink程序");
        System.in.read();
    }
}
