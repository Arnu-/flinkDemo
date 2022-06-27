//package me.arnu.FlinkDemo;
//
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONException;
//import com.alibaba.fastjson.JSONObject;
//import me.arnu.utils.ArnuSign;
//import me.arnu.utils.TextFileSource;
//import org.apache.flink.api.common.functions.FlatJoinFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//
//public class FlinkJoinDemo {
//    private final static Logger logger = LoggerFactory.getLogger("FlinkJoinDemo");
//
//    public static void main(String[] args) throws Exception {
//        ArnuSign.printSign();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        String folderName = "D:\\tmp\\20201211\\jdxm_cctv5.20201211.log";
//        int sleep = 1000;
//        int batch = 5;
//        int showStep = 10;
//        SingleOutputStreamOperator<String> source = env.addSource(new TextFileSource(folderName, sleep, batch, showStep));
//
//
//        // 1、map, 用于输入一个参数产生一个参数，map的功能是对输入的参数进行转换操作。
//        SingleOutputStreamOperator<JSONObject> jsonStream = source.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String s) throws Exception {
//                try {
//                    JSONObject jobj = JSONObject.parseObject(s);
//                    return jobj;
//                } catch (JSONException e) {
//                    logger.error("转换失败：\n" + s, e);
//                }
//                return null;
//            }
//        }).name("将输入字符串变成json对象");
//
//        SplitStream<JSONObject> splitedStream = jsonStream.split(new OutputSelector<JSONObject>() {
//            @Override
//            public Iterable<String> select(JSONObject jsonObject) {
//                if (jsonObject.containsKey("header")) {
//                    JSONObject header = jsonObject.getJSONObject("header");
//                    if (header.containsKey("fileContents")) {
//                        if (header.getString("fileContents").contains("NAV.json")) {
//                            return Collections.singleton("NAV");
//                        } else {
//                            return Collections.singleton("PAGE");
//                        }
//                    }
//                }
//                return Collections.singleton("NULL_TYPE");
//            }
//        });
//        DataStream<JSONObject> navStream = splitedStream.select("NAV");
//        DataStream<JSONObject> pageStream = splitedStream.select("PAGE");
//        DataStream<JSONObject> nullStream = splitedStream.select("NULL_TYPE");
//
//
//        //"fileTime":"2020-12-11 15:01:18","contentsTime":"2020-09-19 20:40:00","fileContents":
//        SingleOutputStreamOperator<Tuple2<String, JSONObject>> navCardStream
//                = navStream.flatMap(new FlatMapFunction<JSONObject, Tuple2<String, JSONObject>>() {
//            @Override
//            public void flatMap(JSONObject jsonObject, Collector<Tuple2<String, JSONObject>> collector) throws Exception {
//                if (jsonObject.containsKey("header")) {
//                    JSONObject header = jsonObject.getJSONObject("header");
//                    if (jsonObject.containsKey("header")) {
//                        JSONObject body = jsonObject.getJSONObject("body");
//                        JSONArray cards = body.getJSONArray("cards");
//                        if (cards != null && cards.size() > 0) {
//                            for (Object c : cards) {
//                                JSONObject card = (JSONObject) c;
//                                //"fileTime":"2020-12-11 15:01:18","contentsTime":"2020-09-19 20:40:00","fileContents":
//                                card.put("fileTime", header.getString("fileTime"));
//                                card.put("contentsTime", header.getString("contentsTime"));
//                                card.put("fileContents", header.getString("fileContents"));
//                                String id = card.getString("id");
//                                if (id != null) {
//                                    collector.collect(Tuple2.of(id, card));
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }).name("取出结构中的数据");
//
//        SingleOutputStreamOperator<Tuple2<String, JSONObject>> pageCardStream
//                = pageStream.flatMap(new FlatMapFunction<JSONObject, Tuple2<String, JSONObject>>() {
//            @Override
//            public void flatMap(JSONObject jsonObject, Collector<Tuple2<String, JSONObject>> collector) throws Exception {
//                if (jsonObject.containsKey("header")) {
//                    JSONObject header = jsonObject.getJSONObject("header");
//                    if (jsonObject.containsKey("header")) {
//                        JSONObject body = jsonObject.getJSONObject("body");
//                        JSONArray cards = body.getJSONArray("cards");
//                        if (cards != null && cards.size() > 0) {
//                            for (Object c : cards) {
//                                JSONObject card = (JSONObject) c;
//                                //"fileTime":"2020-12-11 15:01:18","contentsTime":"2020-09-19 20:40:00","fileContents":
//                                card.put("fileTime", header.getString("fileTime"));
//                                card.put("contentsTime", header.getString("contentsTime"));
//                                card.put("fileContents", header.getString("fileContents"));
//                                String fileContents = header.getString("fileContents");
//                                if (fileContents != null) {
//                                    int start = fileContents.lastIndexOf("/") + 1;
//                                    int end = fileContents.lastIndexOf(".json");
//                                    String fileId = fileContents.substring(start, end);
//                                    if (fileId.length() > 0) {
//                                        collector.collect(Tuple2.of(fileId, card));
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }).name("取出结构中的数据");
//
//        navCardStream.join(pageCardStream)
//                .where(new KeySelector<Tuple2<String, JSONObject>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, JSONObject> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .equalTo(new KeySelector<Tuple2<String, JSONObject>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, JSONObject> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .apply(new FlatJoinFunction<Tuple2<String, JSONObject>, Tuple2<String, JSONObject>, String>() {
//                    @Override
//                    public void join(Tuple2<String, JSONObject> navJson, Tuple2<String, JSONObject> pageJson, Collector<String> collector) throws Exception {
//                        Map<String, Object> resultMap = new HashMap<>();
//                        resultMap.put("navSource", navJson.f1.get("source"));
//                        resultMap.put("navTitle", navJson.f1.get("title"));
//                        resultMap.put("navPageId", navJson.f0);
//                        resultMap.put("source", pageJson.f1.get("source"));
//                        resultMap.put("title", pageJson.f1.get("title"));
//                        resultMap.put("pageId", pageJson.f0);
//                        collector.collect(JSONObject.toJSONString(resultMap));
//                    }
//                }).print();
//
//
//        env.execute("执行flink程序");
//        System.in.read();
//    }
//}
