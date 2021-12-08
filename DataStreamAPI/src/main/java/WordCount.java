import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();
        env.execute();
    }

    //
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //
            String[] words = value.split(",");
            for(String word: words){
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }


}
