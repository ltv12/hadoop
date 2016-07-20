package utils;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.AtomicDouble;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Lev_Stringhacheresiantc on 7/19/2016.
 */
public class MapUtils {
    public static List<String> getTop100(
        final Map<String, Integer> unsortedMap) {

        Comparator<String> comparatorByValue = new Comparator<String>() {
            @Override
            public int compare(String key1, String key2) {
                return -1
                    * unsortedMap.get(key1).compareTo(unsortedMap.get(key2));
            }
        };

        MinMaxPriorityQueue<String> queue = MinMaxPriorityQueue
            .orderedBy(comparatorByValue).maximumSize(100).create();
        queue.addAll(unsortedMap.keySet());

        ArrayList<String> top100 = Lists.newArrayList(queue);
        Collections.sort(top100, comparatorByValue);

        return top100;
    }

    public static void merge(Map<String, Integer> map, String key) {

        if(!map.containsKey(key)){
            map.put(key, 1);
        } else {
            map.put(key, map.get(key) + 1);
        }
    }
}
