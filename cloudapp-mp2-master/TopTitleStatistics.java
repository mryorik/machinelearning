import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.lang.Integer;
import java.util.*;

// Don't Change >>>
public class TopTitleStatistics extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopTitleStatistics(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Title Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(TitleCountMap.class);
        jobA.setReducerClass(TitleCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopTitleStatistics.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Titles Statistics");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopTitlesStatMap.class);
        jobB.setReducerClass(TopTitlesStatReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopTitleStatistics.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
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

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
// <<< Don't Change

    public static class TitleCountMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> stopWords;
        String delimiters;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String stopWordsPath = conf.get("stopwords");
            String delimitersPath = conf.get("delimiters");

            this.stopWords = Arrays.asList(readHDFSFile(stopWordsPath, conf).split("\n"));
            this.delimiters = readHDFSFile(delimitersPath, conf);
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), delimiters);

            Map<String, Integer> words = new HashMap<String, Integer>();

            while (st.hasMoreTokens()) {
                String token = st.nextToken().toLowerCase();
                if (stopWords.contains(token)) {
                    continue;
                }

                Integer c = words.get(token) == null ? 0 : words.get(token);
                words.put(token, ++c);
            }

            for (Map.Entry<String, Integer> e: words.entrySet()) {
                context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
            }
        }
    }

    public static class TitleCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable total = new IntWritable(0);
            for (IntWritable v : values) {
                total.set(total.get() + v.get());
            }

            context.write(key, total);
        }
    }

    public static class TopTitlesStatMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        TreeSet<Pair<Integer, String>> pairs;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            pairs = new TreeSet<Pair<Integer, String>>();
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.valueOf(value.toString(), 10);
            pairs.add(Pair.of(count, key.toString()));
            if (pairs.size() > N) {
                pairs.remove(pairs.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, String> p : pairs) {
                String[] strings = {p.first.toString(), p.second};
                context.write(NullWritable.get(), new TextArrayWritable(strings));
            }
            pairs.clear();
        }
    }

    public static class TopTitlesStatReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        TreeSet<Pair<Integer, String>> pairs;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            pairs = new TreeSet<Pair<Integer, String>>();
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0, mean, max = 0, min = 0, var = 0;

            for (TextArrayWritable v : values) {
                Text[] t = (Text[]) v.toArray();
                Integer c = Integer.valueOf(t[0].toString(), 10);
                String w = t[1].toString();
                pairs.add(Pair.of(c, w));
                if (pairs.size() > N) {
                    pairs.remove(pairs.first());
                }
            }

            for (Pair<Integer, String> p : pairs) {
                sum += p.first;
                if (p.first > max) {
                    max = p.first;
                }
                if (min.equals(0) || p.first < min) {
                    min = p.first;
                }
            }
            mean = sum / N;
            for (Pair<Integer, String> p : pairs) {
                var += Math.pow(p.first - mean, 2);
            }
            var = var / N;

            context.write(new Text("Mean"), new IntWritable(mean));
            context.write(new Text("Sum"), new IntWritable(sum));
            context.write(new Text("Min"), new IntWritable(min));
            context.write(new Text("Max"), new IntWritable(max));
            context.write(new Text("Var"), new IntWritable(var));
        }
    }

}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change