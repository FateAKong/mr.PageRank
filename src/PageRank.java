import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 8:42 PM
 */
public class PageRank {
    public static void main(String[] args) throws Exception {
        JobControl ctrl = new JobControl("PageRank");
        RankCalculator rc = new RankCalculator(args[1], args[2]);
        FileParser fp = new FileParser(args[0], args[1]);
        ControlledJob rcjob = new ControlledJob(rc.getConfig());
        ControlledJob fpjob = new ControlledJob(fp.getConfig());
        rcjob.addDependingJob(fpjob);
        ctrl.addJob(rcjob);
        ctrl.addJob(fpjob);
        ctrl.run();
        while (true) {
            if (ctrl.allFinished()) {
                System.exit(0);
            }
        }
    }
}
