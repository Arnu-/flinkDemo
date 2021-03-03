package me.arnu.FlinkDemo;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Created by betree on 2018/9/14.
 */
public class FlinkManageStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //打开并设置checkpoint
        // 1.设置checkpoint目录，这里我用的是本地路径，记得本地路径要file开头
        // 2.设置checkpoint类型，at lease onece or EXACTLY_ONCE
        // 3.设置间隔时间，同时打开checkpoint功能
        //
        env.setStateBackend(new FsStateBackend("file:///D:\\tmp\\20210225\\"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(20000);
        env.setParallelism(4);


        //添加source 每个2s 发送10条数据，key=1，达到100条时候抛出异常
        env.addSource(new SimpleCheckpointedSource()).name("source接入")
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                // .sum(0).name("1分钟窗口做sum")
                //窗口函数，比如是richwindowsfunction 否侧无法使用manage state
                .apply(new RichWindowFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, Tuple, TimeWindow>() {
                    private transient ValueState<Integer> state;
                    private int count = 0;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Integer, String, Integer>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                        //从state中获取值
                        count = state.value();
                        for (Tuple3<Integer, String, Integer> item : iterable) {
                            count++;
                        }
                        //更新state值
                        state.update(count);
//                        System.out.println("windows:" + tuple.toString() + "  " + count + "   state count:" + state.value());
                        collector.collect(Tuple2.of(count, tuple.toString()));
                    }


                    //获取state
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("##open");
                        ValueStateDescriptor<Integer> descriptor =
                                new ValueStateDescriptor<Integer>(
                                        "average", // the state name
                                        TypeInformation.of(new TypeHint<Integer>() {
                                        }), // type information
                                        0);
                        state = getRuntimeContext().getState(descriptor);
                    }
                }).name("窗口计算")
                .addSink(new SinkFunction<Tuple2<Integer, String>>() {
                    @Override
                    public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
//                        if (value.f0 % 7 == 0) {
//                            throw new Exception("yeyeye!");
//                        }
                        System.out.println(value);
                    }
                }).name("打印输出");
//                .print();
        env.execute();
    }

    /**
     * 实现一个支持Checkpoint的Source
     */
    public static class SimpleCheckpointedSource implements
            SourceFunction<Tuple3<Integer, String, Integer>>
            , CheckpointedFunction
            , CheckpointListener {

        //定义算子实例本地变量，存储Operator数据数量
        private final List<Integer> offset = new ArrayList<>();

        private boolean restored = false;

        //定义operatorState,存储算子的状态值
        private ListState<Integer> operatorState = null;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            operatorState.clear();
            operatorState.addAll(offset);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
//定义并获取operatorState
            ListStateDescriptor<Integer> OperatorDescriptor =
                    new ListStateDescriptor<Integer>(
                            "OperatorState"
                            , TypeInformation.of(new TypeHint<Integer>() {
                    }));
            operatorState = context.getOperatorStateStore().getListState(OperatorDescriptor);
//定义在Restored过程中，从operatorState中回复数据的逻辑
            if (context.isRestored()) {
                offset.clear();
                Iterable<Integer> stateValue = operatorState.get();
                for (Integer v : stateValue) {
                    offset.add(v);
                }
                count = offset.get(offset.size() - 1);
                System.out.println("从检查点恢复咯，count=" + count + " offset size=" + offset.size());
            }
        }

        private Boolean isRunning = true;
        private int count = 0;

        private final String[] keywords = new String[]{
                "arnu",
                "xie",
                "Charlson",
                "Yoncky",
                "Water",
                "Home",
                "DouBao",
                "Meow"
        };


        @Override
        public void run(SourceContext<Tuple3<Integer, String, Integer>> sourceContext) throws Exception {
            while (isRunning) {
                int index = count % keywords.length;
                String name = keywords[index];
                Tuple3<Integer, String, Integer> value = Tuple3.of(1, name, count);
                sourceContext.collect(value);
                count++;
                offset.add(count);

                Date date = new Date();
                if (date.getTime() % 7 == 0) {
                    System.out.println("七秒怪来啦！count="+count + ", offset size=" + offset.size());
                    throw new Exception("123");
                }
                System.out.println("source:" + count + "，state：" + offset.size());
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            offset.clear();
            offset.add(count);
        }
    }

    public static class SimpleSource implements SourceFunction<Tuple3<Integer, String, Integer>> {
        private Boolean isRunning = true;
        private int count = 0;

        private String[] keywords = new String[]{
                "arnu",
                "xie",
                "Charlson",
                "Yoncky",
                "Water",
                "Home",
                "DouBao",
                "Meow"
        };

        @Override
        public void run(SourceContext<Tuple3<Integer, String, Integer>> sourceContext) throws Exception {
            while (isRunning) {
                int index = count % keywords.length;
                String name = keywords[index];
                sourceContext.collect(Tuple3.of(1, name, count));
                count++;

                Date date = new Date();
                if (date.getTime() % 7 == 0) {
                    System.out.println("七秒怪来啦！");
                    throw new Exception("123");
                }
                System.out.println("source:" + count);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}