package cn.tuqikang.flink.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Classname LineSplitter
 * @Description TODO
 * @Date 2022/7/13 11:47 下午
 * @Author 麦阁
 */
public class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        String[] tokens = value.toLowerCase().split("\\W+");

        for (String toke : tokens) {
            if (toke.length() > 0) {
                out.collect(new Tuple2<>(toke, 1));
            }
        }
    }
}
