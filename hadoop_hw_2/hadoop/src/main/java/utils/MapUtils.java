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
    public static Map<String, Integer> getSortedMap(
        final Map<String, Integer> unsortedMap) {

        Comparator<String> stringDescComporator = new Comparator<String>() {
            @Override
            public int compare(String key1, String key2) {
                return -1 * key1.compareTo(key2);
            }
        };
        Map<String, Integer> sortedMap = new TreeMap<>(stringDescComporator);
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

    public static void merge(Map<String, Integer> map, String key) {

        if (!map.containsKey(key)) {
            map.put(key, 1);
        } else {
            map.put(key, map.get(key) + 1);
        }
    }
}
