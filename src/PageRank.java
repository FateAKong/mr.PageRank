import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 8:42 PM
 */
public class PageRank {
    public static void main(String[] args) throws Exception {
        JobControl ctrl = new JobControl("PageRank");
        FileParser fp = new FileParser(args[0], args[1]+"/0");
        ControlledJob fpjob = new ControlledJob(fp.getConfig());
        int iIter = 0;
        boolean isConverged;
        HashMap<String, Double> lastRanks = null;
        do {
            RankCalculator rc = new RankCalculator(args[1]+"/"+Integer.toString(iIter), args[1]+"/"+Integer.toString(iIter+1));;
            ControlledJob rcjob = new ControlledJob(rc.getConfig());
            isConverged = true;
            if (iIter++==0) {
                rcjob.addDependingJob(fpjob);
                ctrl.addJob(rcjob);
                ctrl.addJob(fpjob);
                ctrl.run();
                while (!rcjob.isCompleted());
                lastRanks = rc.getRanks();
            } else {
                ctrl.addJob(rcjob);
                ctrl.run();
                while (!rcjob.isCompleted());
                HashMap<String, Double> curRanks = rc.getRanks();
                for(Entry<String, Double> entry : curRanks.entrySet()) {
                    String key = entry.getKey();
                    Double value = entry.getValue();
                    // TODO move this into MR class
                    if (Math.abs(lastRanks.get(key)-value)>0.00001) {
                        isConverged = false;
                    }
                }
                lastRanks = curRanks;
            }
            System.out.println("current iteration #"+iIter);
        } while (!isConverged);
        // iIter-1
        while (!ctrl.allFinished());
        System.out.println("num of iterations: "+iIter);
        System.exit(0);
    }
}
