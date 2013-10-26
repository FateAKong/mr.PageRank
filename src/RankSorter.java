import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/25/13
 * Time: 10:31 PM
 */
public class RankSorter {
    private Job job = null;

    private static int nTotalOutlinks = 0;

    public Configuration getConfig() {
        return job.getConfiguration();
    }

    public RankSorter(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        job = new Job(new Configuration(), "RankSorter");

        job.setJarByClass(RankSorter.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(PageInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

    }

    private static class Map extends Mapper<Text, PageWritable, DoubleWritable, Text> {
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            nTotalOutlinks ++;
            context.write(new DoubleWritable(value.getRank()), key) ;
        }
    }
    private static class Reduce extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pages = null;
            for (Text value: values) {
                if (pages==null) {
                    pages = value.toString();
                } else {
                    pages += ',' + value.toString();
                }
            }
            context.write(new Text(String.format("%.2f", 100*key.get()/nTotalOutlinks)+'%'), new Text(pages));
        }
    }
}
