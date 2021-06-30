package com.ontotext.trree.plugin.sequences;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents the state of the plugin where a given fingerprint is mapped to a sorted map of sequences.
 * The last 5 fingerprints are kept so that a failed transaction (resulting in a previous fingerprint) can be reverted
 * even after the data is persisted to disk.
 *
 * This is identical to a {@link LinkedHashMap} with the following exceptions:
 * - Only at most MAX_FINGERPRINTS_TO_KEEP keys will be kept
 * - Re-adding an existing key affects the order (equivalent to remove() + put())
 */
public class FingerprintedSequences extends LinkedHashMap<Long, TreeMap<String, Sequence>> {
    private static final int MAX_FINGERPRINTS_TO_KEEP = 5;

    @Override
    protected boolean removeEldestEntry(Map.Entry<Long, TreeMap<String, Sequence>> eldest) {
        return size() > MAX_FINGERPRINTS_TO_KEEP;
    }

    @Override
    public TreeMap<String, Sequence> put(Long key, TreeMap<String, Sequence> value) {
        TreeMap<String, Sequence> previousValue = null;
        if (containsKey(key)) {
            previousValue = remove(key);
        }

        super.put(key, value);

        return previousValue;
    }
}
