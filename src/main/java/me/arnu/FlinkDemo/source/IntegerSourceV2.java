/*

#     __                        
#    /  |  ____ ___  _          
#   / / | / __//   // / /       
#  /_/`_|/_/  / /_//___/        
create @ 2022/11/8                                
*/
package me.arnu.FlinkDemo.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 设计一个可分区的整数输出源，如果只有一个分区，就按顺序一个一个的输出整数
 * 多个分区，就按分区的id，进行取模输出整数
 *
 * @author arnu
 * @date 2022年11月8日
 */
public class IntegerSourceV2 extends RichParallelSourceFunction<Tuple2<Integer, Integer>>
        implements CheckpointedFunction, CheckpointListener {
    private static final Logger logger = LoggerFactory.getLogger(IntegerSourceV2.class);

    /**
     * 这个
     */
    private List<Integer> partitions;

    /**
     * subTaskId
     */
    private int id;

    public IntegerSourceV2() {
        this(6);
    }

    public IntegerSourceV2(int p) {
        this.p = p;
        this.running = false;
        this.partitions = new ArrayList<>();
    }

    private boolean running;

    /**
     * 状态
     */
    private ListState<Tuple2<Integer, Integer>> partitionOffsets;

    /**
     * 输出的数组元素个数
     */
    private final int p;

    /**
     * 该方法用来接收ck完成的通知，一般用来完成一些外部事务，例如commit kafka的offset等。
     * 这个方法在这里用来恢复暂停的数据源。
     *
     * @param checkpointId ckid
     * @throws Exception 可能会抛出异常
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        logger.info("完成了ckId:{}", checkpointId);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("启动数据源的Open");
        RuntimeContext rContext = getRuntimeContext();
        // 得到当前子任务的序号 从0开始
        this.id = rContext.getIndexOfThisSubtask();

        if (partitions == null) {
            partitions = new ArrayList<>();
        }
        if (partitions.size() == 0) {
            // 如果默认状态里是空的。那么说明还没有来得及分配分区到sub task
            // 获取并行度
            int t = rContext.getNumberOfParallelSubtasks();
            if (t == 1) {
                // 并行度等于1的，就直接全都在一个并行度执行
                for (int i = 0; i < p; i++) {
                    partitions.add(i);
                }
            } else {
                // 并行度不为1的，就按顺序分配到各个sub task
                // 计算最后一个节点的元素个数
                // 如果平行度与分区数可以整除，就直接分配。如果不可以整除
                // 那么就用分区数去掉取模后，在平均分配到其他子任务
                // 例如：  7个分区分配到3个子任务， 7%3=1，
                // 最后一个子任务执行1个分区，其他两个子任务执行3个分区
                int b = p % t;
                // 如果是0就是刚好整除
                if (b == 0) {
                    b = p / t;
                }
                // 其他节点的元素个数
                int a = (p - b) / (t - 1);
                int n = a;
                if (id == t - 1) {
                    // 最后一个节点
                    n = b;
                }
                // 分配分区与子任务，按顺序。
                // 第一个子任务（0）得到的分区数是：0，1，2
                // 第二个子任务（1）得到的分区数是：3，4，5
                // 第三个子任务（2）得到的分区数是：6
                for (int i = 0; i < n; i++) {
                    partitions.add((id * a) + i);
                }
            }
        }
        running = true;
    }

    /**
     * 运行数据源
     *
     * @param ctx 上下文
     * @throws Exception 抛出异常
     */
    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
        logger.info("执行 数据源 的 run 方法");
        if (!running) {
            return;
        }
        while (running) {
            Iterable<Tuple2<Integer, Integer>> s = partitionOffsets.get();
            Map<Integer, Integer> m = new HashMap<>();
            for (Tuple2<Integer, Integer> e : s) {
                m.put(e.f0, e.f1);
            }
            List<Tuple2<Integer, Integer>> sList = new ArrayList<>();
            for (Integer key : partitions) {
                if(!m.containsKey(key)){
                    m.put(key, 0);
                }
                m.put(key, m.get(key) + 1);
                Tuple2<Integer, Integer> t = Tuple2.of(key, m.get(key));
                ctx.collect(t);
                sList.add(t);
            }
            partitionOffsets.update(sList);
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    /**
     * 生成快照，更新当前的状态。如果状态是自动更新的就可以不用在这里操作
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        logger.info("做快照, ckId:{}", context.getCheckpointId());

        /*partitionOffsets.clear();
        for (Map.Entry<Integer, Integer> e : state.entrySet()) {
            partitionOffsets.add(Tuple2.of(e.getKey(), e.getValue()));
        }*/
    }

    /**
     * 对状态进行初始化，使得其可以使用
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        int id = getRuntimeContext().getIndexOfThisSubtask();
        // get the state data structure for the per-key state
        partitionOffsets = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("partitionOffsets",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        })));
//        partitionOffsets = context.getOperatorStateStore().getUnionListState(
//                new ListStateDescriptor<>("partitionOffsets",
//                        TypeInformation.of(new TypeHint<List<Integer>>() {
//                        })));
        logger.info("初始化：{}", id);
        if (context.isRestored()) {
            // 是从ck恢复的话，会走到这里
            Iterable<Tuple2<Integer, Integer>> ls = partitionOffsets.get();
            for (Tuple2<Integer, Integer> t : ls) {
                logger.info("恢复数据：id:{}->, p:{}, v:{}", id, t.f0, t.f1);
            }
        }
    }
}
/*



public class MyFunction<T> implements MapFunction<T, T>, CheckpointedFunction {

    private ReducingState<Long> countPerKey;
    private ListState<Long> countPerPartition;

    private long localCount;

    public void initializeState(FunctionInitializationContext context) throws Exception {
        // get the state data structure for the per-key state
        countPerKey = context.getKeyedStateStore().getReducingState(
                new ReducingStateDescriptor<>("perKeyCount", new AddFunction<>(), Long.class));

        // get the state data structure for the per-key state
        countPerPartition = context.getOperatorStateStore().getOperatorState(
                new ListStateDescriptor<>("perPartitionCount", Long.class));

        // initialize the "local count variable" based on the operator state
        for (Long l : countPerPartition.get()) {
            localCount += l;
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // the keyed state is always up to date anyways
        // just bring the per-partition state in shape
        countPerPartition.clear();
        countPerPartition.add(localCount);
    }

    public T map(T value) throws Exception {
        // update the states
        countPerKey.add(1L);
        localCount++;

        return value;
    }
}
*/
