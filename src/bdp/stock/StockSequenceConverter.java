package bdp.stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockSequenceConverter extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        String input = args[0]; 
        String output = args[1];
                        
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        
        job.setJarByClass(StockSequenceConverter.class);
        job.setMapperClass(StockSequenceConverterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DailyStock.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
         
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        
        job.waitForCompletion(true);
        
        return 1;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(),
                new StockSequenceConverter(), args);
        System.exit(res);
    }
}