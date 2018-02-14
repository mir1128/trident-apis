package com.loobo;

import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FakeTweetsBatchSpout implements IBatchSpout {

    private int batchSize;
    public final static String[] ACTORS = { "stefan", "dave", "pere", "nathan", "doug", "ted", "mary", "rose" };
    public final static String[] LOCATIONS = { "Spain", "USA", "Spain", "USA", "USA", "USA", "UK",  "France" };
    public final static String[] SUBJECTS = { "berlin", "justinbieber", "hadoop", "life", "bigdata"};

    private Random randomGenerator;
    private String[] sentences;
    private double[] activityDistribution;
    private double[][] subjectInterestDistribution;

    private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");
    private long tweetId = 0;

    public FakeTweetsBatchSpout() {
        this(5);
    }

    public FakeTweetsBatchSpout(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        System.err.println("Open Spout instance");
        this.randomGenerator = new Random();

        try {
            sentences = IOUtils.readLines(ClassLoader
                    .getSystemClassLoader()
                    .getResourceAsStream("500_sentences_en.txt"))
                    .toArray(new String[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.activityDistribution = getProbabilityDistribution(ACTORS.length, randomGenerator);
        this.subjectInterestDistribution = new double[ACTORS.length][];
        for(int i = 0; i < ACTORS.length; i++) {
            this.subjectInterestDistribution[i] = getProbabilityDistribution(SUBJECTS.length, randomGenerator);
        }
    }

    private double[] getProbabilityDistribution(int n, Random randomGenerator) {
        double a[] = new double[n];
        double s = 0.0d;
        for(int i = 0; i < n; i++) {
            a[i] = 1.0d - randomGenerator.nextDouble();
            a[i] = -1 * Math.log(a[i]);
            s += a[i];
        }
        for(int i = 0; i < n; i++) {
            a[i] /= s;
        }
        return a;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        System.out.println("[batch] FakeTweetsBatchSpout batch started");
        for(int i = 0; i < batchSize; i++) {
            collector.emit(getNextTweet());
        }
    }

    private List<Object> getNextTweet() {
        int actorIndex = randomIndex(activityDistribution, randomGenerator);
        String author = ACTORS[actorIndex];
        String text = sentences[randomGenerator.nextInt(sentences.length)].trim() + " #"
                + SUBJECTS[randomIndex(subjectInterestDistribution[actorIndex], randomGenerator)];

        return Arrays.asList(++tweetId + "", text, author, LOCATIONS[actorIndex], DATE_FORMAT.format(System.currentTimeMillis()));
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("id", "text", "actor", "location", "date");
    }

    private static int randomIndex(double[] distribution, Random randomGenerator) {
        double rnd = randomGenerator.nextDouble();
        double accum = 0;
        int index = 0;
        for(; index < distribution.length && accum < rnd; index++, accum += distribution[index - 1])
            ;
        return index - 1;
    }
}
