import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/25/13
 * Time: 10:31 PM
 */
public class RankSorter {
    private Job job = null;

    private static int nPages = 0;

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
            nPages++;
            context.write(new DoubleWritable(value.getRank()), key) ;
        }
    }
    private static class Reduce extends Reducer<DoubleWritable, Text, Text, Text> {
        // TODO use customized Comparator instead of this
        private LinkedList<String> tops = new LinkedList<String>();
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String rank = String.format("%.6f", 100*key.get()/ nPages)+'%';
            String pages = null;
            for (Text value: values) {
                if (pages==null) {
                    pages = value.toString();
                } else {
                    pages += ", " + value.toString();
                }
            }
            context.write(new Text(rank), new Text(pages));
            tops.addFirst(rank + "\t" + pages);
            if (tops.size()>10) tops.removeLast();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String stats = "";
            Path file = new Path("stat/top10.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream dos = fs.create(file, context);
            for (int i = 0; i<10; i++) {
                stats += Integer.toString(i+1) + '\t' + tops.get(i) + '\n';
            }
            dos.write(stats.getBytes());
            dos.close();
        }
    }
}
