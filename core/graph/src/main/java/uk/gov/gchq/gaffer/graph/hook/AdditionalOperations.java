/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used by the {@link AddOperationsToChain} operation to store details around which
 * operations to add to the chain.
 */
@JsonPropertyOrder(value = {"class", "start", "before", "after", "end"}, alphabetic = true)
public class AdditionalOperations {
    private List<byte[]> start;
    private List<byte[]> end;
    private Map<String, List<byte[]>> before;
    private Map<String, List<byte[]>> after;

    public AdditionalOperations() {
        start = new ArrayList<>();
        end = new ArrayList<>();
        before = new HashMap<>();
        after = new HashMap<>();
    }

    public List<Operation> getStart() {
        return deserialiseOperations(start);
    }

    public void setStart(final List<Operation> start) {
        this.start = serialiseOperations(start);
    }

    public List<Operation> getEnd() {
        return deserialiseOperations(end);
    }

    public void setEnd(final List<Operation> end) {
        this.end = serialiseOperations(end);
    }

    public Map<String, List<Operation>> getBefore() {
        return deserialiseOperations(before);
    }

    public void setBefore(final Map<String, List<Operation>> before) {
        this.before = serialiseOperations(before);
    }

    public Map<String, List<Operation>> getAfter() {
        return deserialiseOperations(after);
    }

    public void setAfter(final Map<String, List<Operation>> after) {
        this.after = serialiseOperations(after);
    }

    private Map<String, List<byte[]>> serialiseOperations(final Map<String, List<Operation>> ops) {
        if (null == ops || ops.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, List<byte[]>> serialisedOps = new HashMap<>(ops.size());
        for (final Map.Entry<String, List<Operation>> entry : ops.entrySet()) {
            serialisedOps.put(SimpleClassNameIdResolver.getClassName(entry.getKey()), serialiseOperations(entry.getValue()));
        }

        return serialisedOps;
    }

    private Map<String, List<Operation>> deserialiseOperations(final Map<String, List<byte[]>> serialisedOps) {
        if (null == serialisedOps || serialisedOps.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, List<Operation>> ops = new HashMap<>(serialisedOps.size());
        for (final Map.Entry<String, List<byte[]>> entry : serialisedOps.entrySet()) {
            ops.put(SimpleClassNameIdResolver.getClassName(entry.getKey()), deserialiseOperations(entry.getValue()));
        }

        return ops;
    }

    private List<byte[]> serialiseOperations(final List<Operation> ops) {
        if (null == ops || ops.isEmpty()) {
            return Collections.emptyList();
        }

        final List<byte[]> serialisedOps = new ArrayList<>(ops.size());
        for (final Operation op : ops) {
            try {
                serialisedOps.add(JSONSerialiser.serialise(op));
            } catch (final SerialisationException e) {
                throw new RuntimeException("Unable to serialise operation: " + op.toString(), e);
            }
        }

        return serialisedOps;
    }

    private List<Operation> deserialiseOperations(final List<byte[]> serialisedOps) {
        if (null == serialisedOps || serialisedOps.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Operation> ops = new ArrayList<>(serialisedOps.size());
        for (final byte[] bytes : serialisedOps) {
            try {
                ops.add(JSONSerialiser.deserialise(bytes, Operation.class));
            } catch (final SerialisationException e) {
                throw new RuntimeException("Unable to deserialise operation", e);
            }
        }
        return ops;
    }
}
