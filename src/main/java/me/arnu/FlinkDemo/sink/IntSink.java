package me.arnu.FlinkDemo.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class IntSink extends RichSinkFunction<Tuple2<Integer, Integer>> {
    @Override
    public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
        super.invoke(value, context);
        RuntimeContext rCtx = getRuntimeContext();
        int subIndex = rCtx.getIndexOfThisSubtask();
        System.out.println("{\"subIndex\":" + subIndex +
                ",\"p\":" + value.f0 +
                ",\"v\":" + value.f1 +
                "}");
    }
}
