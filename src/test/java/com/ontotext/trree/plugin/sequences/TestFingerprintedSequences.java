package com.ontotext.trree.plugin.sequences;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Verifies the contract of {@link FingerprintedSequences} that goes beyond a regular {@link java.util.LinkedHashMap}.
 */
public class TestFingerprintedSequences {
    @Test
    public void testContract() {
        FingerprintedSequences sequences = new FingerprintedSequences();

        // Add 10 fingerprint-sequences
        for (long i = 10; i > 0; i--) {
            assertNull(sequences.put(i, newSequenceMap()));
        }

        assertEquals("Only the last 5 fingerprints should be kept",
                Arrays.asList(5L, 4L, 3L, 2L, 1L), new ArrayList<>(sequences.keySet()));

        assertNotNull(sequences.put(3L, newSequenceMap()));

        assertEquals("Re-adding an existing fingerprint should move it forward",
                Arrays.asList(5L, 4L, 2L, 1L, 3L), new ArrayList<>(sequences.keySet()));
    }

    private TreeMap<String, Sequence> newSequenceMap() {
        TreeMap<String, Sequence> treeMap = new TreeMap<>();
        treeMap.put(UUID.randomUUID().toString(), new Sequence(RandomUtils.nextLong(0, 1_000_000)));
        return treeMap;
    }
}
