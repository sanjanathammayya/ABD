package testCount;


public class countTest {
	
	public static class testWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					
		}
	}
	
	public static class testWordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable > {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {				
		}
	}


	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "my count");

		job.setJarByClass();
		job.setMapperClass();
		job.setReducerClass();

		job.setOutputKeyClass();
		job.setOutputValueClass();

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0: 1);

	}

}
