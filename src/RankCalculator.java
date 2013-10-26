import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 1:04 PM
 */
public class RankCalculator {

    private static final double DAMPING_FACTOR = 0.85;
    private Job job = null;
    private static HashMap<String, Boolean> isRanksConverged = null;

    public Boolean isConverged() {
        return !isRanksConverged.containsValue(false);
    }

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

        isRanksConverged = new HashMap<String, Boolean>();
    }

    private static class Map extends Mapper<Text, PageWritable, Text, RankOrOutlinksWritable> {
        // TODO multiple outlinks with url towards the same page count as one
        // TODO don't count loops (page linking to itself)
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            double rank = value.getRank();
            ArrayList<Text> outlinks = value.getOutlinks();
            if (outlinks.size()>0) {
                rank = rank / outlinks.size();
                for (Text outlink : outlinks) {
                    context.write(outlink, new RankOrOutlinksWritable(rank));
                }
            }
            context.write(key, new RankOrOutlinksWritable(value));
        }
    }

    private static class Reduce extends Reducer<Text, RankOrOutlinksWritable, Text, PageWritable> {
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            isRanksConverged.clear();
//        }

        @Override
        protected void reduce(Text key, Iterable<RankOrOutlinksWritable> values, Context context) throws IOException, InterruptedException {
            double rank = 0;
            double _rank = 0;
            ArrayList<Text> outlinks = null;
            for (RankOrOutlinksWritable value : values) {
                if (value.isRankOrOutlinks) {
                    rank += value.rank;
                } else {
                    outlinks = value.page.getOutlinks();
                    _rank = value.page.getRank();

                    // recover missing PageRank mass for pages (nodes) without outlinks
                    if (outlinks.size()==0) {
                        rank += _rank;
                    }
                }
            }
            rank = 1 - DAMPING_FACTOR + (DAMPING_FACTOR * rank);
            if (rank!=0||outlinks!=null) {
                context.write(key, new PageWritable(rank, outlinks));
            } else {
                throw new NullPointerException("intermediate pairs missing");
            }
            if (Math.abs(rank-_rank)>0.01) {
                isRanksConverged.put(key.toString(), false);
            } else {
                isRanksConverged.put(key.toString(), true);
            }
        }
    }

//    // TODO fix
//    private static class Combine extends Reducer<Text, RankOrOutlinksWritable, Text, RankOrOutlinksWritable> {
//        @Override
//        protected void reduce(Text key, Iterable<RankOrOutlinksWritable> values, Context context) throws IOException, InterruptedException {
//            double rank = 0;
//            for (RankOrOutlinksWritable value : values) {
//                if (value.isRankOrOutlinks) {
//                    rank += value.rank;
//                } else {
//                    context.write(key, new RankOrOutlinksWritable(value.outlinks));
//                }
//            }
//            context.write(key, new RankOrOutlinksWritable(rank));
//        }
//    }

    // there are no objects in static class thus the single class be reused without constructing new instances
    private static class RankOrOutlinksWritable implements Writable {    // used as value class of Mapper output and Reducer input

        private PageWritable page = null;

        private boolean isRankOrOutlinks;
        private double rank; // calculated pagerank value from a particular inlink

        public RankOrOutlinksWritable() {}

        // TODO remove constructor from static class
        public RankOrOutlinksWritable(double rank) {   // for writing results as Mapper output
            this.rank = rank;
            isRankOrOutlinks = true;
        }

        public RankOrOutlinksWritable(PageWritable page) {
            this.page = page;
            isRankOrOutlinks = false;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeBoolean(isRankOrOutlinks);
            if (isRankOrOutlinks) {
                dataOutput.writeDouble(rank);
            } else {
                page.write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            isRankOrOutlinks = dataInput.readBoolean();
            if (isRankOrOutlinks) {
                rank = dataInput.readDouble();
                page = null;
            } else {
                rank = 0;
                page = new PageWritable();
                page.readFields(dataInput);
            }
        }

        @Override
        public String toString() {
            return "RankOrOutlinksWritable{" +
                    "isRankOrOutlinks=" + isRankOrOutlinks +
                    ", rank=" + rank +
                    ", page=" + page +
                    '}';
        }
    }
}