package cn.tuqikang.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Classname DataStreamJobDemo
 * @Description TODO
 * @Date 2022/7/15 9:28 下午
 * @Author 麦阁
 */
public class DataStreamJobDemo {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        //transform
        //(KeySelector<Tuple2<Integer, String>, Integer>) tuple2 -> tuple2.f0
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(t -> t.f0)
                .sum(1);

        //sink
        sum.print();

        env.execute("Tqk-DataStreamJobDemo-Example");
    }
}

