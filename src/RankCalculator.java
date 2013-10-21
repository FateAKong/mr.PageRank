import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 1:04 PM
 */
public class RankCalculator {

    private static final double DAMPING_FACTOR = 0.85;
    private Job job;

    public Configuration getConfig() {
        return job.getConfiguration();
    }

    public RankCalculator(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        job = new Job(new Configuration(), "RankCalculator");

        job.setJarByClass(RankCalculator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RankOrOutlinksWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
//        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(PageInputFormat.class);
        job.setOutputFormatClass(PageOutputFormat.class);
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    private static class Map extends Mapper<Text, PageWritable, Text, RankOrOutlinksWritable> {
        // TODO multiple outlinks with url towards the same page count as one
        // TODO don't count loops (page linking to itself)
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            double rank = value.getRank();
            ArrayList<Text> outlinks = value.getOutlinks();
            rank = rank / outlinks.size();
            for (Text outlink : outlinks) {
                context.write(new Text(outlink), new RankOrOutlinksWritable(rank));
            }
            RankOrOutlinksWritable row = new RankOrOutlinksWritable(outlinks);
            context.write(key, row);
        }
    }

    private static class Reduce extends Reducer<Text, RankOrOutlinksWritable, Text, PageWritable> {
        @Override
        protected void reduce(Text key, Iterable<RankOrOutlinksWritable> values, Context context) throws IOException, InterruptedException {
            double rank = 0;
            ArrayList<Text> outlinks = null;
            for (RankOrOutlinksWritable value : values) {
                if (value.isRankOrOutlinks) {
                    rank += value.rank;
                } else {
                    outlinks = new ArrayList<Text>(value.outlinks);
                }
            }
            rank = 1 - DAMPING_FACTOR + (DAMPING_FACTOR * rank);
            context.write(key, new PageWritable(rank, outlinks));
        }
    }

    // TODO fix
    private static class Combine extends Reducer<Text, RankOrOutlinksWritable, Text, RankOrOutlinksWritable> {
        @Override
        protected void reduce(Text key, Iterable<RankOrOutlinksWritable> values, Context context) throws IOException, InterruptedException {
            double rank = 0;
            for (RankOrOutlinksWritable value : values) {
                if (value.isRankOrOutlinks) {
                    rank += value.rank;
                } else {
                    context.write(key, new RankOrOutlinksWritable(value.outlinks));
                }
            }
            context.write(key, new RankOrOutlinksWritable(rank));
        }
    }

    private static class RankOrOutlinksWritable implements Writable {    // used as value class of Mapper output and Reducer input
        private boolean isRankOrOutlinks;

        private double rank; // calculated pagerank value from a particular inlink
        private ArrayList<Text> outlinks = null;

        // for reading results from Mapper output into Reducer input
        // note that the object will be reused without reconstructing thus do initialization in write()
        private RankOrOutlinksWritable() {
        }

        private RankOrOutlinksWritable(double rank) {   // for writing results as Mapper output
            this.rank = rank;
            isRankOrOutlinks = true;
        }

        private RankOrOutlinksWritable(ArrayList<Text> outlinks) {  // for writing results as Mapper output
            this.outlinks = outlinks;
            isRankOrOutlinks = false;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeBoolean(isRankOrOutlinks);
            if (isRankOrOutlinks) {
                dataOutput.writeDouble(rank);
            } else {
                dataOutput.writeInt(outlinks.size());
                for (Text outlink: outlinks) {
                    dataOutput.writeUTF(outlink.toString());
                }
            }
//            String line = Boolean.toString(isRankOrOutlinks);
//            if (isRankOrOutlinks) {
//                line += ' ' + Double.toString(rank);
//            } else {
//                for (Text outlink : outlinks) {
//                    line += ' ' + outlink.toString();
//                }
//            }
//            line += '\n';
//            dataOutput.writeUTF(line);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            outlinks = new ArrayList<Text>();
            isRankOrOutlinks = dataInput.readBoolean();
            if (isRankOrOutlinks) {
                rank = dataInput.readDouble();
            } else {
                int nOutlinks = dataInput.readInt();
                while (nOutlinks-- > 0) {
                    outlinks.add(new Text(dataInput.readUTF()));
                }
            }
//            String[] pieces = dataInput.readLine().split("\\s");
//            isRankOrOutlinks = Boolean.parseBoolean(pieces[0].trim());
//            if (isRankOrOutlinks) {
//                rank = Double.parseDouble(pieces[1].trim());
//            } else {
//                for (int i = 1; i < pieces.length; i++) outlinks.add(new Text(pieces[i].trim()));
//            }
        }
    }
}