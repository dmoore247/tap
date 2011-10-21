package tap.formats.sample;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class SampleOutputFormat extends TextOutputFormat<SampleRec, NullWritable> {

    @Override
    public RecordWriter<SampleRec, NullWritable> getRecordWriter(
            FileSystem fs, JobConf job, String name, Progressable prog)
            throws IOException {
        
        // this is a LineRecordWriter
        final RecordWriter writer = super.getRecordWriter(fs, job, name, prog);
        
        return new RecordWriter<SampleRec, NullWritable>() {
            public void write(SampleRec rec, NullWritable ignore) throws IOException {
                Text text = new Text();
                text.set("SAMPLE: word=" + rec.word + ",count=" + rec.count);
                writer.write(text, null);
            }
            public void close(Reporter reporter) throws IOException {
                writer.close(reporter);
            }
        };
    }
}
