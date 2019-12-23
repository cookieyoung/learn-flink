package one;

import one.hotItempackage.CountAgg;
import one.hotItempackage.ItemViewCount;
import one.hotItempackage.TopNHotItems;
import one.hotItempackage.UserBehavior;
import one.hotItempackage.WindowResultFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        URL userBehaviorUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(userBehaviorUrl.toURI()));
        PojoTypeInfo<UserBehavior> pojoTypeInfo =
            (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior>
            pojoCsvInputFormat =
            new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldOrder);
        /**
         * 创建数据源
         */
        DataStream<UserBehavior> dataSource = env.createInput(pojoCsvInputFormat, pojoTypeInfo);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior userBehavior) {
                    /**
                     * 转换成毫秒
                     */
                    return userBehavior.timestamp * 1000;
                }
            });
        /**
         * 过滤
         */
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                /**
                 * 过滤出来点击事件
                 */
                return "pv".equals(userBehavior.behavior);
            }
        });
        /**
         * 统计窗口数据
         */
        DataStream<ItemViewCount>
            windowData =
            pvData.keyBy("itemId").timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());
        /**
         * 计算top
         */
        DataStream<String> topData=windowData.keyBy("windowEnd").process(new TopNHotItems(3));
        topData.print();
        env.execute("Hot Items Job");
    }
}
