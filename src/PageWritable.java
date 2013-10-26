/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 2:50 PM
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PageWritable implements Writable {   // used as value class of Mapper input and Reducer output

    private double rank = 0;
    private ArrayList<Text> outlinks = null;

    public  PageWritable() {
        outlinks = new ArrayList<Text>();
    }

    public PageWritable(double rank, ArrayList<Text> outlinks) {
        this.rank = rank;
        this.outlinks = outlinks;
    }

    @Override
    public String toString() {
        return "PageWritable{" +
                "rank=" + rank +
                ", outlinks=" + outlinks +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(rank);
        dataOutput.writeInt(outlinks.size());
        for(Text outlink: outlinks) {
            outlink.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rank = dataInput.readDouble();
        int nOutlinks = dataInput.readInt();
        while (nOutlinks-->0) {
            Text outlink = new Text();
            outlink.readFields(dataInput);
            outlinks.add(outlink);
        }
//        System.out.println("PWr#"+this);
    }

    public double getRank() {
        return rank;
    }

    public ArrayList<Text> getOutlinks() {
        return outlinks;
    }
}