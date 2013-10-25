import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/24/13
 * Time: 2:33 PM
 */
public class JobController implements Runnable {
    private JobControl jobControl;

    public JobController(JobControl jobControl) {
        this.jobControl = jobControl;
    }

    @Override
    public void run() {
        jobControl.run();
    }
}
