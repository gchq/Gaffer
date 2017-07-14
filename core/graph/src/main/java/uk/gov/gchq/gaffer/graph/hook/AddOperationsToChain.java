/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.graph.hook;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * A <code>AddOperationsToChain</code> is a {@link GraphHook} that allows a
 * user to insert additional operations at certain points on the operation chain.
 * At the start, before a specific Operation, after a specific Operation, or at the end.
 */
public class AddOperationsToChain implements GraphHook {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    private List<Operation> start;
    private List<Operation> end;
    private Map<String, List<Operation>> before;
    private Map<String, List<Operation>> after;

    /**
     * Default constructor.
     */
    public AddOperationsToChain() {
    }

    /**
     * Constructs a {@link AddOperationsToChain} using the path to json
     * file specifying Operations to add.
     *
     * @param addOperationsPath Path to file containing Operations to add.
     * @throws IOException if the file reading fails.
     */
    public AddOperationsToChain(final String addOperationsPath) throws IOException {

        Path path = Paths.get(addOperationsPath);
        AddOperationsToChain addOperations;
        if (path.toFile().exists()) {
            addOperations = fromJson(Files.readAllBytes(path));
        } else {
            addOperations = fromJson(StreamUtil.openStream(OperationDeclarations.class, addOperationsPath));
        }
        setOperations(addOperations);
    }

    /**
     * Constructs a {@link AddOperationsToChain} using the json byte[]
     * specifying Operations to add.
     *
     * @param addOperationsBytes byte[] containing Operations to add.
     * @throws IOException if the byte[] fails to be deserialised.
     */
    public AddOperationsToChain(final byte[] addOperationsBytes) throws IOException {
        setOperations(fromJson(addOperationsBytes));
    }

    public List<Operation> getStart() {
        return start;
    }

    public void setStart(final List<Operation> start) {
        this.start = start;
    }

    public List<Operation> getEnd() {
        return end;
    }

    public void setEnd(final List<Operation> end) {
        this.end = end;
    }

    public Map<String, List<Operation>> getBefore() {
        return before;
    }

    public void setBefore(final Map<String, List<Operation>> before) {
        this.before = before;
    }

    public Map<String, List<Operation>> getAfter() {
        return after;
    }

    public void setAfter(final Map<String, List<Operation>> after) {
        this.after = after;
    }

    /**
     * Adds in the additional Operations specified.  The original opChain will
     * be updated.
     *
     * @param opChain the {@link OperationChain} being executed.
     * @param user    the {@link User} executing the operation chain
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        OperationChain<?> newOpChain = new OperationChain<>();

        if (start != null) {
            newOpChain.getOperations().addAll(start);
        }
        if (opChain != null) {
            for (final Operation originalOp : opChain.getOperations()) {

                if (before != null) {
                    List<Operation> beforeOps = before.get(originalOp.getClass().getName());
                    if (beforeOps != null) {
                        newOpChain.getOperations().addAll(beforeOps);
                    }
                }

                newOpChain.getOperations().add(originalOp);

                if (after != null) {
                    List<Operation> afterOps = after.get(originalOp.getClass().getName());
                    if (afterOps != null) {
                        newOpChain.getOperations().addAll(afterOps);
                    }
                }
            }
        }
        if (end != null) {
            newOpChain.getOperations().addAll(end);
        }
        opChain.getOperations().clear();
        opChain.getOperations().addAll(newOpChain.getOperations());
    }

    @Override
    public <T> T postExecute(final T result,
                             final OperationChain<?> opChain, final User user) {
        return result;
    }

    private static AddOperationsToChain fromJson(final byte[] json) {
        try {
            return JSON_SERIALISER.deserialise(json, AddOperationsToChain.class);
        } catch (final SerialisationException e) {
            throw new SchemaException("Failed to load element definitions from bytes", e);
        }
    }

    private static AddOperationsToChain fromJson(final InputStream inputStream) {
        try {
            return JSON_SERIALISER.deserialise(inputStream, AddOperationsToChain.class);
        } catch (final SerialisationException e) {
            throw new SchemaException("Failed to load element definitions from bytes", e);
        }
    }

    private void setOperations(final AddOperationsToChain addOperations) {
        if (addOperations.getStart() != null) {
            this.setStart(addOperations.getStart());
        }
        if (addOperations.getEnd() != null) {
            this.setEnd(addOperations.getEnd());
        }
        if (addOperations.getBefore() != null) {
            this.setBefore(addOperations.getBefore());
        }
        if (addOperations.getAfter() != null) {
            this.setAfter(addOperations.getAfter());
        }
    }

}
