package edu.osu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.HCatTableInfo;

public class RdTxtToHcat extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			word.set(value);
			context.write(word, word);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text,
    Text, HCatRecord> {


      @Override
      protected void reduce(Text key,Iterable<Text> values,Context context)
        throws IOException ,InterruptedException {
          HCatRecord record = new DefaultHCatRecord(1);
          record.set(0, key);
          context.write(null, record);
        }
    }
	
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String serverUri = args[0];
        String inputFilePath = args[1];
        String outputTableName = args[2];
        String dbName = null;

        String principalID = System
                .getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
        if (principalID != null)
            conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
        Job job = new Job(conf, "RdTxtToHcat");
        job.setJarByClass(RdTxtToHcat.class);
        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        HCatOutputFormat.setOutput(job, HCatTableInfo.getOutputTableInfo(serverUri, principalID, dbName, outputTableName,null));
        HCatSchema s = HCatOutputFormat.getTableSchema(job);
        System.err.println("INFO: output schema explicitly set for writing:"
                + s);
        HCatOutputFormat.setSchema(job, s);
        job.setOutputFormatClass(HCatOutputFormat.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RdTxtToHcat(), args);
        System.exit(exitCode);
    }
	
}
