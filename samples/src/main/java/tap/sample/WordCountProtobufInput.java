package tap.sample;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import tap.core.*;

public class WordCountProtobufInput extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        /* Set up a basic pipeline of map reduce */
        Assembly wordcount = new Assembly(getClass()).named("wordcount");
        /* Parse options - just use the standard options - input and output location, time window, etc. */
        BaseOptions o = new BaseOptions();
        int result = o.parse(wordcount, args);
        if (result != 0)
            return result;
        if (o.input == null) {
            System.err.println("Must specify input directory");
            return 1;
        }
        if (o.output == null) {
            System.err.println("Must specify output directory");
            return 1;
        }

        Pipe input = Pipe.of(Protos.CountRec.class).at(o.input).protoFormat();
        Pipe counts = Pipe.of(Protos.CountRec.class).at(o.output).protoFormat();
        wordcount.produces(counts);
        
        Phase count = new Phase().reads(input).writes(counts).map(Mapper.class).
            groupBy("word").reduce(Reducer.class);
        
        
        if (o.forceRebuild) wordcount.forceRebuild();
        if (o.dryRun) {
            wordcount.dryRun();
            return 0;
        }
        
        wordcount.execute();
        
        return 0;
    }

    public static class CountRec {
        public String word;
        public int count;
    }
    

    public static class Mapper extends BaseMapper<ProtobufWritable<Protos.CountRec>,CountRec> {
        @Override
        public void map(
                ProtobufWritable<Protos.CountRec> in,
                CountRec out,
                TapContext<CountRec> context) {
            out.word = in.get().getWord();
            out.count = 1;
            context.write(out);
        }        
    }

    public static class Reducer extends BaseReducer<CountRec,ProtobufWritable<Protos.CountRec>> {
        
        // ProtobufWritable<Protos.CountRec> protoWritable = ProtobufWritable.newInstance(Protos.CountRec.class);
        
        @Override
        public void reduce(Iterable<CountRec> in,
                ProtobufWritable<Protos.CountRec> out,
                TapContext<ProtobufWritable<Protos.CountRec>> context) {
            
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if(word == null)
                    word = rec.word;
                count += rec.count;
            }
            
            out.setConverter(Protos.CountRec.class);
            out.set(Protos.CountRec.newBuilder()
                    .setWord(word)
                    .setCount(count)
                    .build());
            context.write(out);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountProtobufInput(), args);
        System.exit(res);
    }

}
