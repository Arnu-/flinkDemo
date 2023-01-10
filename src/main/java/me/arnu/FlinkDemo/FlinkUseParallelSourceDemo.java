package me.arnu.FlinkDemo;

import me.arnu.FlinkDemo.sink.IntSink;
import me.arnu.FlinkDemo.source.IntegerSource;
import me.arnu.FlinkDemo.source.IntegerSourceV2;
import me.arnu.utils.CustomLocalStreamEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkUseParallelSourceDemo {
    public static void main(String[] args) throws Exception {
        String chkBasePath = "file:///E:\\tmp\\20230110\\chk";
        String savepointPath = chkBasePath +
//                "\\1138bc9934753440638f762716be214c\\chk-6";
//        "\\a11357113e3d88931f9d9b01cefa2991\\chk-2";
//        "\\f1b46a1d719d8683b7c29e444ec7523a\\chk-4";
        "\\98c040bb2af82bd81d818b78848f7fef\\chk-5";
        boolean restoreState = false;
        // boolean restoreState = true;
        int parallelism = 4;
        boolean allowNonRestoredState = true;
        SavepointRestoreSettings savepointSettings =
                SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);

        // HeartBeat.Timeout
        Configuration conf = new Configuration();
        conf.setInteger("HeartBeat.Timeout", 600000);
        if (!conf.contains(RestOptions.PORT)) {
            // explicitly set this option so that it's not set to 0 later
            conf.setInteger(RestOptions.PORT, RestOptions.PORT.defaultValue());
        }
        CustomLocalStreamEnvironment env = new CustomLocalStreamEnvironment(conf);

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(chkBasePath);

        env.getCheckpointConfig()
                . setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(parallelism);
        //SingleOutputStreamOperator<Tuple2<Integer, Integer>> s = env.addSource(new IntegerSource())
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> s = env.addSource(new IntegerSourceV2())
                // .setParallelism(2)
                .name("数据源");
        s.keyBy(r->r.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .max(1)
                .addSink(new IntSink()).name("输出各个分区的数据");

        if (restoreState) {
            env.execute("恢复点启动", savepointSettings);
        } else {
            env.execute("启动");
        }
        System.in.read();
    }
}
