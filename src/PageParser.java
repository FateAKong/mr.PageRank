import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 4:08 PM
 */
public class PageParser {

    private Job job = null;

    public Configuration getConfig() {
        return job.getConfiguration();
    }

    public PageParser(String inputPath, String outputPath) throws IOException {
        job = new Job(new Configuration(), "PageParser");

        job.setJarByClass(PageParser.class);

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
            // it's wrong to eliminate nodes without outlinks which results in incorrect initial probability (1/N)
            if (pieces.length > 1) {
                for (int i = 1; i < pieces.length; i++) {
                    if (!pieces[i].equals(pieces[0])) {
                        context.write(new Text(pieces[0]), new Text(pieces[i]));
                    }
                }
            } else {
                context.write(new Text(pieces[0]), new Text());
            }
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, PageWritable> {

        private int nPages = 0, nTotalOutlinks = 0, nMinOutlinks = -1, nMaxOutlinks = -1;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            nPages++;
            HashSet<Text> hashOutlinks = new HashSet<Text>();
            ArrayList<Text> outlinks = null;
            boolean isSink = false;
            for (Text value : values) {
                if (value.getLength() == 0) {
                    isSink = true;
                } else {
                    hashOutlinks.add(new Text(value));
                }
            }
            if (isSink) {
                outlinks = new ArrayList<Text>();
                context.write(key, new PageWritable(1, outlinks));
            } else {
                outlinks = new ArrayList<Text>(hashOutlinks);
                context.write(key, new PageWritable(1, outlinks));
            }
            int nOutlinks = outlinks.size();
            nTotalOutlinks += nOutlinks;
            if (nMinOutlinks==-1&& nMaxOutlinks ==-1) {
                nMinOutlinks = nMaxOutlinks = nOutlinks;
            } else {
                if (nOutlinks<nMinOutlinks) nMinOutlinks = nOutlinks;
                if (nOutlinks>nMaxOutlinks) nMaxOutlinks = nOutlinks;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String stats = "";
            Path file = new Path("stat/graph_property.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream dos = fs.create(file, context);
            stats += "number of nodes == " + nPages + "\n";
            stats += "number of edges == " + nTotalOutlinks + "\n";
            stats += "out-degree of each node" + "\n";
            stats += "\tmin == " + nMinOutlinks +"\n";
            stats += "\tmax == " + nMaxOutlinks + "\n";
            stats += "\tavg == " + String.format("%.2f", ((float)nTotalOutlinks)/nPages) + "\n";
            dos.write(stats.getBytes());
            dos.close();
        }
    }
}
