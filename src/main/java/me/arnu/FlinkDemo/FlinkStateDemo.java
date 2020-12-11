package me.arnu.FlinkDemo;

import me.arnu.utils.ArnuSign;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FlinkStateDemo {
    public static void main(String[] args) throws Exception {
        ArnuSign.printSign();
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<Long> input=env.fromElements(1L,2L,3L,4L,7L,5L,1L,5L,4L,6L,1L,7L,8L,9L,1L);

        input.flatMap(new OperatorStateMap()).setParallelism(1).print();

        System.out.println(env.getExecutionPlan());

        env.execute();
    }


    public static class OperatorStateMap extends RichFlatMapFunction<Long, Tuple2<Integer,String>> implements CheckpointedFunction {

        //托管状态
        private ListState<Long> listState;
        //原始状态
        private List<Long> listElements;

        @Override
        public void flatMap(Long value, Collector collector) throws Exception {
            if(value==1){
                if(listElements.size()>0){
                    StringBuffer buffer=new StringBuffer();
                    for(Long ele:listElements){
                        buffer.append(ele+" ");
                    }
                    int sum=listElements.size();
                    collector.collect(new Tuple2<Integer,String>(sum,buffer.toString()));
                    listElements.clear();
                }
            }else{
                listElements.add(value);
            }
        }

        /**
         * 进行checkpoint进行快照
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            for(Long ele:listElements){
                listState.add(ele);
            }
        }

        /**
         * state的初始状态，包括从故障恢复过来
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor listStateDescriptor=new ListStateDescriptor("checkPointedList",
                    TypeInformation.of(new TypeHint<Long>() {}));
            listState=context.getOperatorStateStore().getListState(listStateDescriptor);
            //如果是故障恢复
            if(context.isRestored()){
                //从托管状态将数据到移动到原始状态
                for(Long ele:listState.get()){
                    listElements.add(ele);
                }
                listState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listElements=new ArrayList<Long>();
        }
    }
}
