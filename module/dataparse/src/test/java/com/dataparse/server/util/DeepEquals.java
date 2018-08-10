package com.dataparse.server.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeepEquals {

    /**
     * An implementation of "deep equals" for collection classes, built in the
     * Arrays.deepEquals() style. It attempts to compare equality based on the
     * contents of the collection.
     *
     * @param t1 -
     *            first object, most likely a collection of some sort.
     * @param t2 -
     *            second object, most likely a collection of some sort.
     * @return - true if the content of the collections are equal.
     */
    public static <T> boolean deepEquals(T t1, T t2) {

        if (t1 == t2) {
            return true;
        }

        if (t1 == null || t2 == null) {
            return false;
        }

        if (t1 instanceof Map && t2 instanceof Map) {
            return mapDeepEquals((Map<?, ?>) t1, (Map<?, ?>) t2);
        } else if (t1 instanceof List && t2 instanceof List) {
            return linearDeepEquals((List<?>) t1, (List<?>) t2);

        } else if (t1 instanceof Set && t2 instanceof Set) {
            return linearDeepEquals((Set<?>) t1, (Set<?>) t2);

        } else if (t1 instanceof Object[] && t2 instanceof Object[]) {
            return linearDeepEquals((Object[]) t1, (Object[]) t2);

        } else {
            return t1.equals(t2);
        }
    }

    /**
     * Compares two maps for equality. This is based around the idea that if the
     * keys are deep equal and the values the keys return are deep equal then
     * the maps are equal.
     *
     * @param m1 -
     *            first map
     * @param m2 -
     *            second map
     * @return - weather the maps are deep equal
     */
    private static boolean mapDeepEquals(Map<?, ?> m1, Map<?, ?> m2) {
        if (m1.size() != m1.size()) {
            return false;
        }

        Set<?> allKeys = m1.keySet();
        if (!linearDeepEquals(allKeys, m2.keySet())) {
            return false;
        }

        for (Object key : allKeys) {
            if (!deepEquals(m1.get(key), m2.get(key))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two Collections for deep equality.
     */
    private static boolean linearDeepEquals(Collection<?> s1, Collection<?> s2) {
        if (s1.size() != s2.size()) {
            return false;
        }

        for (Object s1Item : s1) {
            boolean found = false;
            for (Object s2Item : s2) {
                if (deepEquals(s2Item, s1Item)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two Object[] for deep equality
     */
    private static boolean linearDeepEquals(Object[] s1, Object[] s2) {

        if (s1.length != s2.length) {
            return false;
        }

        for (Object s1Item : s1) {
            boolean found = false;
            for (Object s2Item : s2) {
                if (deepEquals(s2Item, s1Item)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }
}
