package com.loobo;

import org.apache.commons.collections.MapUtils;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class Demo {

    public static class PereTweetsFilter implements Filter {

        int partitionIndex;

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            this.partitionIndex = context.getPartitionIndex();
        }

        @Override
        public void cleanup() {
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            boolean filter = tuple.getString(1).equals("pere");
            if(filter) {
                System.err.println("I am partition [" + partitionIndex + "] and I have filtered pere.");
            }
            return filter;
        }
    }

    public static class PerActorTweetsFilter extends BaseFilter {

        private int partitionIndex;
        private String actor;

        public PerActorTweetsFilter(String actor) {
            this.actor = actor;
        }
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            System.out.println("prepare PerActorTweetsFilter instance");
            this.partitionIndex = context.getPartitionIndex();
        }
        @Override
        public boolean isKeep(TridentTuple tuple) {
            boolean filter = tuple.getString(1).equals(actor);
            if(filter) {
                System.err.println("I am partition [" + partitionIndex + "] and I have kept a tweet by: " + actor);
            }
            return filter;
        }
    }

    public static class UppercaseFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            collector.emit(new Values(tuple.getString(0).toUpperCase()));
        }
    }

    public static class LocationAggregator implements Aggregator<Map<String, Integer>> {

        private int partitionId;

        @Override
        public Map<String, Integer> init(Object batchId, TridentCollector collector) {
            System.out.println("[batch] LocationAggregator init");
            return new HashMap<String, Integer>();
        }

        @Override
        public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
            String location = tuple.getString(0);
            val.put(location, MapUtils.getInteger(val, location, 0) + 1);
        }

        @Override
        public void complete(Map<String, Integer> val, TridentCollector collector) {
            collector.emit(new Values(val));
            System.out.println("[batch] LocationAggregator completed map is  " + val);
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            this.partitionId = context.getPartitionIndex();
            System.out.println("[batch] LocationAggregator prepare partitionId is " + partitionId);
        }

        @Override
        public void cleanup() {

        }
    }

}
