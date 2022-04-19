package com.ontotext.trree.plugin.sequences;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.InitReason;
import com.ontotext.trree.sdk.PatternInterpreter;
import com.ontotext.trree.sdk.PluginBase;
import com.ontotext.trree.sdk.PluginConnection;
import com.ontotext.trree.sdk.PluginException;
import com.ontotext.trree.sdk.PluginTransactionListener;
import com.ontotext.trree.sdk.RequestContext;
import com.ontotext.trree.sdk.ShutdownReason;
import com.ontotext.trree.sdk.StatementIterator;
import com.ontotext.trree.sdk.UpdateInterpreter;
import gnu.trove.TLongObjectHashMap;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;

/**
 * GraphDB Sequences plugin main class
 */
public class SequencesPlugin extends PluginBase implements PluginTransactionListener, PatternInterpreter, UpdateInterpreter {
    private static final String NS = "http://www.ontotext.com/plugins/sequences#";

    private static final String CREATE_LOCAL_NAME = "create";
    private static final String DROP_LOCAL_NAME = "drop";
    private static final String RESET_LOCAL_NAME = "reset";
    private static final String PREPARE_LOCAL_NAME = "prepare";
    private static final String NEXT_VALUE_LOCAL_NAME = "nextValue";
    private static final String CURRENT_VALUE_LOCAL_NAME = "currentValue";

    private static final IRI RESET_IRI = SimpleValueFactory.getInstance().createIRI(NS, RESET_LOCAL_NAME);

    private long createSequenceId;
    private long dropSequenceId;
    private long resetSequenceId;
    private long prepareSequenceId;
    private long nextValueId;
    private long currentValueId;

    private volatile boolean preparedForUse;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final TLongObjectHashMap<Sequence> sequencesById = new TLongObjectHashMap<>();

    private final TreeMap<String, Sequence> sequencesByIRI = new TreeMap<>();

    private Path statePath;
    private final FingerprintedSequences fingerprintedSequences = new FingerprintedSequences();
    private long expectedFingerprint;

    @Override
    public String getName() {
        return "sequences";
    }

    @Override
    public void initialize(InitReason reason, PluginConnection pluginConnection) {
        super.initialize(reason, pluginConnection);
        createSequenceId = newSystemIri(pluginConnection, CREATE_LOCAL_NAME);
        dropSequenceId = newSystemIri(pluginConnection, DROP_LOCAL_NAME);
        resetSequenceId = newSystemIri(pluginConnection, RESET_LOCAL_NAME);
        prepareSequenceId = newSystemIri(pluginConnection, PREPARE_LOCAL_NAME);
        nextValueId = newSystemIri(pluginConnection, NEXT_VALUE_LOCAL_NAME);
        currentValueId = newSystemIri(pluginConnection, CURRENT_VALUE_LOCAL_NAME);
        statePath = getDataDir().toPath().resolve("state.js");
        readStateFromDisk(pluginConnection);
    }

    @Override
    public void shutdown(ShutdownReason reason) {
        sequencesById.clear();
        sequencesByIRI.clear();
        fingerprintedSequences.clear();
    }

    @Override
    public void transactionStarted(PluginConnection pluginConnection) {
    }

    @Override
    public void transactionCommit(PluginConnection pluginConnection) {
        if (preparedForUse) {
            sequencesById.forEachValue(sequence -> {
                sequence.prepare();
                return true;
            });

            saveStateToDisk();
        }
    }

    @Override
    public void transactionCompleted(PluginConnection pluginConnection) {
        if (preparedForUse) {
            preparedForUse = false;
            sequencesById.forEachValue(sequence -> {
                sequence.commit();
                return true;
            });
        }
    }

    @Override
    public void transactionAborted(PluginConnection pluginConnection) {
        if (preparedForUse) {
            preparedForUse = false;
            sequencesById.forEachValue(sequence -> {
                sequence.rollback();
                return true;
            });
        }
    }

    @Override
    public double estimate(long subject, long predicate, long object, long context, PluginConnection pluginConnection, RequestContext requestContext) {
        if (subject == Entities.UNBOUND || object == Entities.BOUND) {
            return Double.POSITIVE_INFINITY;
        } else {
            return 1;
        }
    }

    @Override
    public StatementIterator interpret(long subject, long predicate, long object, long context, PluginConnection pluginConnection, RequestContext requestContext) {
        if (predicate == nextValueId || predicate == currentValueId) {
            if (!preparedForUse || pluginConnection.getTransactionId() == 0) {
                throw new PluginException("Sequences must be prepared before use in transaction");
            }

            if (subject == 0) {
                return StatementIterator.EMPTY;
            }

            Sequence sequence = sequencesById.get(subject);
            if (sequence == null) {
                throw new PluginException("No such sequence: " + pluginConnection.getEntities().get(subject));
            }

            long value;
            if (predicate == nextValueId) {
                value = sequence.nextValue();
                // Add a statement that resets the sequence to the last obtained value (+1 because reset will subtract 1)
                // via a statement that will preserve the semantics of not modifying the state of a plugin via a query.
                // This statement will also be the sole sequence changing trigger when the transaction is replayed
                // in a cluster environment.
                pluginConnection.getRepository().addStatement((Resource) pluginConnection.getEntities().get(subject),
                        RESET_IRI, SimpleValueFactory.getInstance().createLiteral(value + 1));
            } else {
                value = sequence.currentValue();
            }

            return StatementIterator.create(subject, predicate,
                    pluginConnection.getEntities()
                            .put(SimpleValueFactory.getInstance().createLiteral(value), Entities.Scope.REQUEST), context);
        }

        return null;
    }

    @Override
    public long[] getPredicatesToListenFor() {
        return new long[] {createSequenceId, dropSequenceId, prepareSequenceId, resetSequenceId};
    }

    @Override
    public boolean interpretUpdate(long subject, long predicate, long object, long context, boolean isAddition, boolean isExplicit, PluginConnection pluginConnection) {
        if (predicate == createSequenceId) {
            Value subjectValue = pluginConnection.getEntities().get(subject);

            // Convert request-scoped ID to system-scope
            subject = pluginConnection.getEntities().put(subjectValue, Entities.Scope.SYSTEM);
            if (sequencesById.contains(subject)) {
                throw new PluginException("Sequence " + subjectValue + " already exists");
            }

            Sequence sequence = new Sequence(parseNumber(pluginConnection, object));
            sequencesById.put(subject, sequence);
            sequencesByIRI.put(subjectValue.stringValue(), sequence);

            getLogger().debug("Created sequence {}", subjectValue);
        } else if (predicate == dropSequenceId) {
            Value subjectValue = pluginConnection.getEntities().get(subject);
            sequencesById.remove(subject);
            sequencesByIRI.remove(subjectValue.stringValue());

            getLogger().debug("Removed sequence {}", subjectValue);
        } else if (predicate == resetSequenceId) {
            Value subjectValue = pluginConnection.getEntities().get(subject);
            Sequence sequence = sequencesById.get(subject);
            if (sequence == null) {
                throw new PluginException("Sequence " + subjectValue + " does not exist");
            }

            long value = parseNumber(pluginConnection, object);
            sequence.setValue(value);

            getLogger().debug("Set sequence {} to value {}", subjectValue, value);
        } else if (predicate == prepareSequenceId) {
            getLogger().debug("Prepared sequences");
        }

        // All of these prepare the sequences for use
        preparedForUse = true;

        return true;
    }

    @Override
    public long getFingerprint() {
        if (sequencesByIRI.isEmpty()) {
            return 0;
        }
        return sequencesByIRI.hashCode();
    }

    @Override
    public void setFingerprint(long fingerprint) {
        expectedFingerprint = fingerprint;
    }

    private long newSystemIri(PluginConnection pluginConnection, String localName) {
        return pluginConnection.getEntities().put(SimpleValueFactory.getInstance().createIRI(NS, localName), Entities.Scope.SYSTEM);
    }

    private long parseNumber(PluginConnection pluginConnection, long valueId) {
        Value value = pluginConnection.getEntities().get(valueId);
        if (value instanceof BNode) {
            return 0;
        } else if (value instanceof Literal) {
            try {
                return ((Literal) value).longValue() - 1;
            } catch (NumberFormatException e) {
                throw new PluginException("Provided sequence start is not a number: " + value);
            }
        } else {
            throw new PluginException("Provided sequence start is not a number: " + value);
        }
    }

    private void saveStateToDisk() {
        try {
            long fingerprint = getFingerprint();
            TreeMap<String, Sequence> sequencesByIRICopy = new TreeMap<>();
            copySequenceMap(sequencesByIRI, sequencesByIRICopy);
            fingerprintedSequences.put(fingerprint, sequencesByIRICopy);
            Files.createDirectories(statePath.getParent());
            objectMapper.writeValue(statePath.toFile(), fingerprintedSequences);
        } catch (IOException e) {
            throw new PluginException("Unable to save sequence state", e);
        }
    }

    private void readStateFromDisk(PluginConnection pluginConnection) {
        if (Files.exists(statePath)) {
            try {
                fingerprintedSequences.putAll(objectMapper.readValue(statePath.toFile(), FingerprintedSequences.class));
            } catch (IOException e) {
                throw new PluginException("Unable to restore sequences from disk", e);
            }
        }
        if (expectedFingerprint != 0) {
            TreeMap<String, Sequence> storedSequencesByIRI = fingerprintedSequences.get(expectedFingerprint);
            if (storedSequencesByIRI == null) {
                throw new PluginException("Expected sequences fingerprint not found in stored state");
            }

            copySequenceMap(storedSequencesByIRI, sequencesByIRI);
            sequencesByIRI.forEach((iri, sequence) -> {
                long id = pluginConnection.getEntities().put(SimpleValueFactory.getInstance().createIRI(iri), Entities.Scope.SYSTEM);
                sequencesById.put(id, sequence);
            });
        }
    }

    private void copySequenceMap(TreeMap<String, Sequence> sourceMap, TreeMap<String, Sequence> targetMap) {
        sourceMap.forEach((iri, sequence) -> targetMap.put(iri, sequence.copy()));
    }
}
