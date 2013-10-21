import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 4:08 PM
 */
public class FileParser {

    private Job job;

    public Configuration getConfig() {
        return job.getConfiguration();
    }

    public FileParser(String inputPath, String outputPath) throws IOException {
        job = new Job(new Configuration(), "FileParser");

        job.setJarByClass(FileParser.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(PageOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] pieces = value.toString().split("\\s");
            for (int i = 1; i < pieces.length; i++) {   // length > 1
                if (!pieces[i].equals(pieces[0])) {
                    context.write(new Text(pieces[0]), new Text(pieces[i]));
                }
            }
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, PageWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<Text> hashOutlinks = new HashSet<Text>();
            for (Text value: values) {
                hashOutlinks.add(new Text(value));
            }
            context.write(key, new PageWritable(1, new ArrayList<Text>(hashOutlinks)));
        }
    }

//    private static class OutlinksWritable implements Writable {
//        private ArrayList<Text> outlinks;
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            //To change body of implemented methods use File | Settings | File Templates.
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//            //To change body of implemented methods use File | Settings | File Templates.
//        }
//    }

}
