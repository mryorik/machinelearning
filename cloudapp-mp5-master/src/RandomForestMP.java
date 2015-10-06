import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.tree.RandomForest;


import java.util.HashMap;
import java.util.regex.Pattern;

public final class RandomForestMP {
    private static class ParsePoint implements Function<String, LabeledPoint> {
        private static final Pattern SPACE = Pattern.compile(",");

        public LabeledPoint call(String line) {
            String[] tok = SPACE.split(line);
            double[] point = new double[tok.length-1];
            for (int i = 0; i < tok.length - 1; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }
            return new LabeledPoint(Double.parseDouble(tok[tok.length - 1]), Vectors.dense(point));
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
                    "Usage: RandomForestMP <training_data> <test_data> <results>");
            System.exit(1);
        }
        String training_data_path = args[0];
        String test_data_path = args[1];
        String results_path = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("RandomForestMP");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final RandomForestModel model;

        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

		// TODO
        SparkConf sparkConf = new SparkConf().setAppName("RandomForest MP");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(training_data_path);
        JavaRDD<LabeledPoint> input = lines.map(new ParsePoint());

        lines = sc.textFile(test_data_path);
        JavaRDD<LabeledPoint> test = lines.map(new ParsePoint());

        model = RandomForestModel.trainClassifier(input, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);

        JavaRDD<LabeledPoint> results = test.map(new Function<Vector, LabeledPoint>() {
            public LabeledPoint call(Vector points) {
                return new LabeledPoint(model.predict(points), points);
            }
        });

        results.saveAsTextFile(results_path);

        sc.stop();
    }

}
