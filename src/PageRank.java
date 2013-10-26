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
        JobController ctrller = new JobController(ctrl);
        PageParser fp = new PageParser(args[0], args[1]+"/0");
        ControlledJob fpjob = new ControlledJob(fp.getConfig());
        int iIter = 0;
        boolean isConverged;
        do {
            System.out.println("begin iteration #"+iIter);
            RankCalculator rc = new RankCalculator(args[1]+"/"+Integer.toString(iIter), args[1]+"/"+Integer.toString(iIter+1));
            ControlledJob rcjob = new ControlledJob(rc.getConfig());
            isConverged = true;
            if (iIter==0) {
                rcjob.addDependingJob(fpjob);
                ctrl.addJob(rcjob);
                ctrl.addJob(fpjob);
            } else {
                ctrl.addJob(rcjob);
            }
            Thread t = new Thread(ctrller);
            t.start();
            while (!ctrl.allFinished()) {
                System.out.println("running iteration #"+iIter);
                Thread.sleep(5000);
            }
            System.out.println("end iteration #"+iIter);
            isConverged = rc.isConverged();
            iIter++;
        } while (!isConverged);

        RankSorter rs = new RankSorter(args[1]+"/"+Integer.toString(iIter), args[2]);
        ControlledJob rsjob = new ControlledJob(rs.getConfig());
        ctrl.addJob(rsjob);
        Thread t = new Thread(ctrller);
        t.start();
        while (!ctrl.allFinished()) {
            System.out.println("sorting");
            Thread.sleep(5000);
        }
        System.out.println("num of iterations: "+iIter);
        System.exit(0);
    }
}