package com.ontotext.trree.plugin.sequences;

import com.ontotext.graphdb.Config;
import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.test.functional.base.SingleRepositoryFunctionalTest;
import com.ontotext.test.utils.OwlimSeRepositoryDescription;
import com.ontotext.trree.graphdb.GraphDBRepositoryFactory;
import com.ontotext.trree.graphdb.GraphDBSailFactory;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryConfig;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for using the Sequences plugin
 */
public class TestSequencesPlugin extends SingleRepositoryFunctionalTest {
    @ClassRule
    public static TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();

    @BeforeClass
    public static void setWorkDir() {
        System.setProperty("graphdb.home.work", String.valueOf(tmpFolder.getRoot()));
        Config.reset();
    }

    @AfterClass
    public static void resetWorkDir() {
        System.clearProperty("graphdb.home.work");
        Config.reset();
    }
    @Override
    protected RepositoryConfig createRepositoryConfiguration() {
        final OwlimSeRepositoryDescription repositoryDescription = new OwlimSeRepositoryDescription();
        final OwlimSeRepositoryDescription.OWLIMSailConfigEx sailConfig = repositoryDescription.getOwlimSailConfig();
        sailConfig.setRuleset("empty");
        sailConfig.setType(GraphDBSailFactory.SAIL_TYPE);
        SailRepositoryConfig sailrepositoryConfig = new SailRepositoryConfig(sailConfig);
        sailrepositoryConfig.setType(GraphDBRepositoryFactory.REPOSITORY_TYPE);
        return repositoryDescription.getRepositoryConfig();
    }

    @Test
    public void testUse() {
        runCreate(null, 15L);
        runUseTest(1, 15, true);
        runUseTest(4, 17, true);
    }

    @Test
    public void testUseAfterRestart() {
        runCreate(null, 15L);
        runUseTest(1, 15, true);
        restartRepository();
        runUseTest(4, 17, true);
    }

    @Test
    public void testUseAfterRollback() {
        runCreate(3L, null);
        runUseTest(3, 1, false);
        runUseTest(3, 1, true);
    }

    @Test
    public void testUseAfterRollbackAndRestart() {
        runCreate(3L, null);
        runUseTest(3, 1, false);
        restartRepository();
        runUseTest(3, 1, true);
    }

    @Test
    public void testUseAfterReset() {
        runCreate(null, 30L);
        runUseTest(1, 30, true);
        resetSequence("urn:myseq1", null);
        runUseTest(1, 32, true);
        resetSequence("urn:myseq2", 200L);
        runUseTest(4, 200, true);
    }

    @Test
    public void testUseAfterDrop() {
        runCreate(null, 50L);
        runUseTest(1, 50, true);
        dropSequence("urn:myseq1");
        try {
            runUseTest(1, 50, true);
            fail("Must fail with exception");
        } catch (Exception e) {
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("No such sequence: urn:myseq1"));
        }
    }

    @Test
    public void testConcurrentUseCommit() throws InterruptedException, ExecutionException {
        runCreate(null, 70L);
        runConcurrentUse(1, 70, true);
    }

    @Test
    public void testConcurrentUseRollback() throws InterruptedException, ExecutionException {
        runCreate(10L, null);
        runConcurrentUse(10, 1, false);
    }

    private void runConcurrentUse(long sequence1, long sequence2, boolean commit) throws InterruptedException {
        try (RepositoryConnection connection = getRepository().getConnection()) {
            connection.begin();
            prepareSequences(connection);
            assertEquals(sequence1, getNextValue(connection, "urn:myseq1"));
            assertEquals(sequence2, getNextValue(connection, "urn:myseq2"));

            AtomicReference<Throwable> otherTxError = new AtomicReference<>();
            Thread t2 = new Thread(() -> {
                try {
                    if (commit) {
                        // Concurrent connection committed
                        runUseTest(sequence1 + 2, sequence2 + 2, true);
                    } else {
                        // Concurrent connection rolled back
                        runUseTest(sequence1, sequence2, true);
                    }
                } catch (Exception | Error e) {
                    otherTxError.set(e);
                }
            });
            t2.setDaemon(true);
            t2.start();

            Thread.sleep(500);
            assertEquals(sequence1 + 1, getNextValue(connection, "urn:myseq1"));
            assertEquals(sequence2 + 1, getNextValue(connection, "urn:myseq2"));

            if (commit) {
                connection.commit();
            } else {
                connection.rollback();
            }

            t2.join(5000);
            if (t2.isAlive()) {
                fail("Thread must finish within the allotted time");
            }
            assertNull(otherTxError.get());
        }
    }

    private void runCreate(Long sequence1, Long sequence2) {
        try (RepositoryConnection connection = getRepository().getConnection()) {
            connection.begin();
            createSequence(connection, "urn:myseq1", sequence1);
            createSequence(connection, "urn:myseq2", sequence2);
            connection.commit();
        }
    }

    private void runUseTest(long sequence1, long sequence2, boolean commit) {
        try (RepositoryConnection connection = getRepository().getConnection()) {
            connection.begin();
            prepareSequences(connection);
            assertEquals(sequence1, getNextValue(connection, "urn:myseq1"));
            assertEquals(sequence1 + 1, getNextValue(connection, "urn:myseq1"));
            assertEquals(sequence1 + 1, getCurrentValue(connection, "urn:myseq1"));
            assertEquals(sequence1 + 1, getCurrentValue(connection, "urn:myseq1"));
            assertEquals(sequence1 + 1, getCurrentValue(connection, "urn:myseq1"));
            assertEquals(sequence2, getNextValue(connection, "urn:myseq2"));
            assertEquals(sequence1 + 1, getCurrentValue(connection, "urn:myseq1"));
            assertEquals(sequence1 + 2, getNextValue(connection, "urn:myseq1"));
            assertEquals(sequence2, getCurrentValue(connection, "urn:myseq2"));
            assertEquals(sequence2 + 1, getNextValue(connection, "urn:myseq2"));
            if (commit) {
                connection.commit();
            } else {
                connection.rollback();
            }
        }
    }

    private void prepareSequences(RepositoryConnection connection) {
        connection.prepareUpdate("insert data { [] <http://www.ontotext.com/plugins/sequences#prepare> [] }")
                .execute();
    }

    private void printSequence(RepositoryConnection connection, String sequenceId) {
        try (TupleQueryResult tqr = connection.prepareTupleQuery("select ?s {" +
                //"values ?a { 1 2 3 } ." +
                "{select ?s { <" + sequenceId + "> <http://www.ontotext.com/plugins/sequences#nextValue> ?s } }" +
                "}").evaluate()) {
            while (tqr.hasNext()) {
                System.out.println(sequenceId + " -> " + tqr.next());
            }
        }
    }

    private long getNextValue(RepositoryConnection connection, String sequenceId) {
        try (TupleQueryResult tqr = connection.prepareTupleQuery("select ?value {" +
                "<" + sequenceId + "> <http://www.ontotext.com/plugins/sequences#nextValue> ?value" +
                "}").evaluate()) {
            assertTrue(tqr.hasNext());
            Value value = tqr.next().getValue("value");
            assertTrue(value instanceof Literal);
            assertEquals(XSD.LONG, ((Literal) value).getDatatype());
            long longValue = ((Literal) value).longValue();
            assertFalse(tqr.hasNext());
            return longValue;
        }
    }

    private long getCurrentValue(RepositoryConnection connection, String sequenceId) {
        try (TupleQueryResult tqr = connection.prepareTupleQuery("select ?value {" +
                "<" + sequenceId + "> <http://www.ontotext.com/plugins/sequences#currentValue> ?value" +
                "}").evaluate()) {
            assertTrue(tqr.hasNext());
            Value value = tqr.next().getValue("value");
            assertTrue(value instanceof Literal);
            assertEquals(XSD.LONG, ((Literal) value).getDatatype());
            long longValue = ((Literal) value).longValue();
            assertFalse(tqr.hasNext());
            return longValue;
        }
    }

    private void createSequence(RepositoryConnection connection, String sequenceName, Long startValue) {
        String startValueString = startValue == null ? "[]" : startValue.toString();
        connection.prepareUpdate(
                "insert data { <" + sequenceName + "> <http://www.ontotext.com/plugins/sequences#create> "
                        + startValueString + " }")
                .execute();
    }

    private void resetSequence(String sequenceName, Long startValue) {
        try (RepositoryConnection connection = getRepository().getConnection()) {
            resetSequence(connection, sequenceName, startValue);
        }
    }

    private void resetSequence(RepositoryConnection connection, String sequenceName, Long startValue) {
        String startValueString = startValue == null ? "[]" : startValue.toString();
        connection.prepareUpdate(
                "insert data { <" + sequenceName + "> <http://www.ontotext.com/plugins/sequences#reset> "
                        + startValueString + " }")
                .execute();
    }

    private void dropSequence(String sequenceName) {
        try (RepositoryConnection connection = getRepository().getConnection()) {
            dropSequence(connection, sequenceName);
        }
    }

    private void dropSequence(RepositoryConnection connection, String sequenceName) {
        connection.prepareUpdate(
                "insert data { <" + sequenceName + "> <http://www.ontotext.com/plugins/sequences#drop> [] }").execute();
    }

    private void restartRepository() {
        getRepository().shutDown();
        getRepository().init();
    }
}
