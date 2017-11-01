package bdp.stock;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

enum CustomCounters {NUM_COMPANIES}

public class StockYearJoin {
    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setJarByClass(StockYearJoin.class);
        job.setMapperClass(StockYearJoinMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(TextIntPair.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(0);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.addCacheFile(new Path("/data/companylist.tsv").toUri());

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
        }
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}