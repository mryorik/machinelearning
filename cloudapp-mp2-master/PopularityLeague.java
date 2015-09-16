import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

	public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "LinkCount");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJar("PopularityLeague.jar");
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "PopularityLeague");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);
        jobB.setMapperClass(PopularityLeagueMap.class);
        jobB.setReducerClass(PopularityLeagueReduce.class);
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setJar("PopularityLeague.jar");
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), " \t:");

            if (st.hasMoreTokens()) {
                Integer id = Integer.valueOf(st.nextToken(), 10);

                while (st.hasMoreTokens()) {
                    Integer linkedId = Integer.valueOf(st.nextToken(), 10);
                    context.write(new IntWritable(linkedId.intValue()), new IntWritable(1));
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int c = 0;
            for (IntWritable v : values) {
                c++;
            }
            context.write(key, new IntWritable(c));
        }
    }

    public static class PopularityLeagueMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
    	List<String> leagueIds;

    	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            this.leagueIds = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer id = Integer.valueOf(key.toString(), 10);
            if (leagueIds.contains(id.toString())) {
            	Integer count = Integer.valueOf(value.toString(), 10);
            	Integer[] integers = {id, count};
            	context.write(NullWritable.get(), new IntArrayWritable(integers));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,InterruptedException {
        }
    }

    public static class PopularityLeagueReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
    	Map<Integer, Integer> league;
    	Map<Integer, Integer> ranks;

    	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
        	league = new HashMap<Integer, Integer>();
        	ranks = new HashMap<Integer, Integer>();
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable v : values) {
                IntWritable[] ii = (IntWritable[]) v.toArray();
                int id = ii[0].get();
                int count = ii[1].get();

                league.put(id, count);
                ranks.put(id, 0);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Integer> l1 : league.entrySet()) {
                for (Map.Entry<Integer, Integer> l2 : league.entrySet()) {
                    if (l2.getValue() < l1.getValue()) {
                        Integer currRank = ranks.get(l1.getKey());
                        ranks.put(l1.getKey(), currRank + 1);
                    }
                }
            }

            for (Map.Entry<Integer, Integer> r : ranks.entrySet()) {
                context.write(new IntWritable(r.getKey().intValue()), new IntWritable(r.getValue().intValue()));
            }
        }
    }
}
