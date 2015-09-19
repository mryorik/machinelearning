import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
    private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
    private int N;

    private long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words


    ------------------------------------------------- */
        String word = (String) tuple.getValueByField("word");
        Integer count = (Integer) tuple.getValueByField("count");

        if (currentTopWords.size() < N) {
            currentTopWords.put(word, count);
        } else {
            Iterator<String> it = currentTopWords.keySet().iterator();
            String minCountWord = it.next();
            Integer minCount = currentTopWords.get(minCountWord);
            while (it.hasNext()) {
                String w = it.next();

                if (currentTopWords.get(w) < minCount) {
                    minCountWord = w;
                    minCount = currentTopWords.get(w);
                }
            }
            if (minCount < count) {
                currentTopWords.remove(minCountWord);
                currentTopWords.put(word, count);
            }
        }

        //reports the top N words periodically
        if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
            collector.emit(new Values(printMap()));
            lastReportTime = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("top-N"));

    }

    public String printMap() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("top-words = [ ");
        for (String word : currentTopWords.keySet()) {
            stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(",");
        stringBuilder.deleteCharAt(lastCommaIndex + 1);
        stringBuilder.deleteCharAt(lastCommaIndex);
        stringBuilder.append("]");
        return stringBuilder.toString();

    }
}
