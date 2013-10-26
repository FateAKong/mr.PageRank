import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import sun.net.util.URLUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 6:06 PM
 */
public class PageInputFormat extends FileInputFormat<Text, PageWritable> {
    @Override
    public RecordReader<Text, PageWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        taskAttemptContext.setStatus(inputSplit.toString());
        RecordReader<Text, PageWritable> ret = new PageRecordReader();
        ret.initialize(inputSplit, taskAttemptContext);
        return ret;
    }

    private static class PageRecordReader extends RecordReader<Text, PageWritable> {

        private LineRecordReader lineReader;
        private Text currentKey = null;
        private PageWritable currentValue = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            lineReader = new LineRecordReader();
            lineReader.initialize(inputSplit, taskAttemptContext);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (lineReader.nextKeyValue()) {
                Text lineValue = lineReader.getCurrentValue();
                // uft-8 encoding may go wrong here because of toString() of the entire line just read
                String[] pieces = lineValue.toString().split("\\s");
                if (pieces.length >= 3) {
                    ArrayList<Text> outlinks = new ArrayList<Text>();
                    for (int i = 3; i<pieces.length; i++) {
                        outlinks.add(new Text(pieces[i].trim()));
                    }
                    currentKey = new Text(pieces[1]);
                    currentValue = new PageWritable(Double.parseDouble(pieces[2]), outlinks);
                    return true;
                } else {
                    throw new IOException("incorrect file format");
                }
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public PageWritable getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineReader.close();
        }
    }
}
