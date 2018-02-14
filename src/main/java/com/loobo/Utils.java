package com.loobo;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    @SuppressWarnings({"serial", "rawtypes"})
    public static class PrintFilter implements Filter {

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
        }

        @Override
        public void cleanup() {
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            System.out.println("[batch] " + tuple);
            return true;
        }
    }


    public final static Map<String, Integer> getTopNOfMap(Map<String, Integer> map, int n) {

        List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());

        entryList.addAll(map.entrySet());
        Collections.sort(entryList, (arg0, arg1) -> arg1.getValue().compareTo(arg0.getValue()));

        Map<String, Integer> toReturn = new HashMap<>();
        for (Map.Entry<String, Integer> entry : entryList.subList(0, Math.min(entryList.size(), n))) {
            toReturn.put(entry.getKey(), entry.getValue());
        }
        return toReturn;
    }
}
