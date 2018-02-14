package com.loobo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import java.io.IOException;

public class Skeleton {

    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(100);

        TridentTopology topology = new TridentTopology();
//        topology.newStream("spout", spout).each(new Fields("id", "text", "actor", "location", "date"), new Utils.PrintFilter());

//        topology.newStream("filter", spout)
//                .aggregate(new Fields("location"), new Demo.LocationAggregator(), new Fields("location_counts"))
//                .each(new Fields("location_counts"), new Utils.PrintFilter());

//        topology.newStream("spout", spout)
//                .partitionBy(new Fields("location"))
//                .partitionAggregate(new Fields("location"), new Demo.LocationAggregator(), new Fields("location_counts"))
//                .parallelismHint(3)
//                .each(new Fields("location_counts"), new Utils.PrintFilter());

        topology.newStream("spout", spout)
                .groupBy(new Fields("location"))
                .aggregate(new Fields("location"), new Count(), new Fields("count"))
                .each(new Fields("location", "count"), new Utils.PrintFilter());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hackaton", conf, buildTopology(drpc));
    }

}
