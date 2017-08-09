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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A <code>AddOperationsToChain</code> is a {@link GraphHook} that allows a
 * user to insert additional operations at certain points on the operation chain.
 * At the start, before a specific Operation, after a specific Operation, or at the end.
 * A user can also specify authorised Operations to add, and if the user has
 * the opAuths, the additional Operations will be added to the chain.
 */
public class AddOperationsToChain implements GraphHook {
    private final AdditionalOperations defaultOperations = new AdditionalOperations();
    private final LinkedHashMap<String, AdditionalOperations> authorisedOps = new LinkedHashMap<>();

    /**
     * Adds in the additional Operations specified.  The original opChain will
     * be updated.
     *
     * @param opChain the {@link OperationChain} being executed.
     * @param user    the {@link User} executing the operation chain
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        final List<Operation> newOpList = new ArrayList<>();

        boolean hasAuth = false;
        if (!authorisedOps.isEmpty() && !user.getOpAuths().isEmpty()) {
            for (final String auth : authorisedOps.keySet()) {
                if (user.getOpAuths().contains(auth)) {
                    newOpList.addAll(addOperationsToChain(opChain, authorisedOps.get(auth)));
                    hasAuth = true;
                    break;
                }
            }
        }

        if (!hasAuth) {
            newOpList.addAll(addOperationsToChain(opChain, defaultOperations));
        }
        opChain.getOperations().clear();
        opChain.getOperations().addAll(newOpList);
    }

    @Override
    public <T> T postExecute(final T result,
                             final OperationChain<?> opChain, final User user) {
        return result;
    }

    public void setStart(final List<Operation> start) {
        this.defaultOperations.setStart(start);
    }

    public List<Operation> getStart() {
        return defaultOperations.getStart();
    }

    public void setEnd(final List<Operation> end) {
        this.defaultOperations.setEnd(end);
    }

    public List<Operation> getEnd() {
        return defaultOperations.getEnd();
    }

    public void setBefore(final Map<String, List<Operation>> before) {
        this.defaultOperations.setBefore(before);
    }

    public Map<String, List<Operation>> getBefore() {
        return defaultOperations.getBefore();
    }

    public void setAfter(final Map<String, List<Operation>> after) {
        this.defaultOperations.setAfter(after);
    }

    public Map<String, List<Operation>> getAfter() {
        return defaultOperations.getAfter();
    }

    public void setAuthorisedOps(final LinkedHashMap<String, AdditionalOperations> authorisedOps) {
        this.authorisedOps.clear();
        if (authorisedOps != null) {
            this.authorisedOps.putAll(authorisedOps);
        }
    }

    public LinkedHashMap<String, AdditionalOperations> getAuthorisedOps() {
        return authorisedOps;
    }

    private List<Operation> addOperationsToChain(final OperationChain<?> opChain, final AdditionalOperations additionalOperations) {
        List<Operation> newOpList = new ArrayList<>();

        newOpList.addAll(additionalOperations.getStart());
        if (opChain != null && !opChain.getOperations().isEmpty()) {
            for (final Operation originalOp : opChain.getOperations()) {
                List<Operation> beforeOps = additionalOperations.getBefore().get(originalOp.getClass().getName());
                if (beforeOps != null) {
                    newOpList.addAll(beforeOps);
                }
                newOpList.add(originalOp);
                List<Operation> afterOps = additionalOperations.getAfter().get(originalOp.getClass().getName());
                if (afterOps != null) {
                    newOpList.addAll(afterOps);
                }
            }
        }
        newOpList.addAll(additionalOperations.getEnd());

        return newOpList;
    }
}
