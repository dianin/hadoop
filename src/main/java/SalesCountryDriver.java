import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class SalesCountryDriver {



    public static void main(String[] args) {

        JobClient jobClient = new JobClient();

        JobConf jobConf = new JobConf(SalesCountryDriver.class);

        jobConf.setJobName("SalePerCountry");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(SalesMapper.class);
        jobConf.setReducerClass(SalesCountryReducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path("C:\\Users\\igord\\IdeaProjects\\HadoopTesting\\SalesJan2009.csv"));
        FileOutputFormat.setOutputPath(jobConf, new Path("C:\\Users\\igord\\IdeaProjects\\HadoopTesting\\"));

        jobClient.setConf(jobConf);

        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
