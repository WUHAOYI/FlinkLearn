package com.why.Cases.TopN;

import com.why.Entity.Event;
import com.why.Source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by WHY on 2025/2/17.
 * Functions: 全局TopN
 * 需求 统计最近10 秒钟内最热门的两个 url 链接，并且每 5 秒钟更新一次
 * 处理方法 ProcessAllWindowFunction 利用全窗口函数进行处理
 */
public class AllWindowTopN {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 全局开窗 并行度为1

        // 2. 读取数据源 添加水位线
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 3. 开窗 根据需求开一个大小为10s 步长为5s的滑动窗口
        SingleOutputStreamOperator<String> processStream = dataStream.map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.url;
                    }
                }).windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        // 进行计数
                        HashMap<String, Long> countUrl = new HashMap<>();
                        for (String element : elements) {
                            long newCount = countUrl.getOrDefault(element, 0L) + 1L;
                            countUrl.put(element, newCount);
                        }

                        // 将HashMap中的数据添加到ArrayList中
                        ArrayList<Tuple2<String, Long>> countUrlList = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : countUrl.entrySet()) {
                            countUrlList.add(Tuple2.of(entry.getKey(), entry.getValue()));
                        }

                        // 利用ArrayList进行排序
                        countUrlList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });

                        //排序完进行输出
                        StringBuilder result = new StringBuilder();
                        result.append("=========================================\n");
                        result.append("窗口开始时间: " + new Timestamp(context.window().getStart()) + "\n");
                        for (int i = 0; i < 2 && i < countUrlList.size(); i++) {
                            Tuple2<String, Long> item = countUrlList.get(i);
                            String info = "浏览量No." + (i + 1) + " url:" + item.f0 + " 访问次数:" + item.f1 + "\n";
                            result.append(info);
                        }
                        result.append("窗口结束时间: " + new Timestamp(context.window().getEnd()) + "\n");
                        result.append("=========================================\n");
                        out.collect(result.toString());
                    }


                });

        processStream.print();
        env.execute();

    }
}
