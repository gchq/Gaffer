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
import uk.gov.gchq.gaffer.store.Context;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code AddOperationsToChain} is a {@link GraphHook} that allows a
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
     * @param context    the {@link Context} executing the operation chain
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        final List<Operation> newOpList = new ArrayList<>();

        boolean hasAuth = false;
        if (!authorisedOps.isEmpty() && !context.getUser().getOpAuths().isEmpty()) {
            for (final String auth : authorisedOps.keySet()) {
                if (context.getUser().getOpAuths().contains(auth)) {
                    final AdditionalOperations additionalOperations = authorisedOps.get(auth);

                    newOpList.addAll(additionalOperations.getStart());
                    newOpList.addAll(addOperationsToChain(opChain, additionalOperations));
                    newOpList.addAll(additionalOperations.getEnd());

                    hasAuth = true;
                    break;
                }
            }
        }

        if (!hasAuth) {
            newOpList.addAll(defaultOperations.getStart());
            newOpList.addAll(addOperationsToChain(opChain, defaultOperations));
            newOpList.addAll(defaultOperations.getEnd());
        }
        opChain.getOperations().clear();
        opChain.getOperations().addAll(newOpList);
    }

    @Override
    public <T> T postExecute(final T result,
                             final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    public List<Operation> getStart() {
        return defaultOperations.getStart();
    }

    public void setStart(final List<Operation> start) {
        this.defaultOperations.setStart(start);
    }

    public List<Operation> getEnd() {
        return defaultOperations.getEnd();
    }

    public void setEnd(final List<Operation> end) {
        this.defaultOperations.setEnd(end);
    }

    public Map<String, List<Operation>> getBefore() {
        return defaultOperations.getBefore();
    }

    public void setBefore(final Map<String, List<Operation>> before) {
        this.defaultOperations.setBefore(before);
    }

    public Map<String, List<Operation>> getAfter() {
        return defaultOperations.getAfter();
    }

    public void setAfter(final Map<String, List<Operation>> after) {
        this.defaultOperations.setAfter(after);
    }

    public LinkedHashMap<String, AdditionalOperations> getAuthorisedOps() {
        return authorisedOps;
    }

    public void setAuthorisedOps(final LinkedHashMap<String, AdditionalOperations> authorisedOps) {
        this.authorisedOps.clear();
        if (null != authorisedOps) {
            this.authorisedOps.putAll(authorisedOps);
        }
    }

    private List<Operation> addOperationsToChain(final OperationChain<?> opChain, final AdditionalOperations additionalOperations) {
        final List<Operation> opList = new ArrayList<>();

        if (null != opChain && !opChain.getOperations().isEmpty()) {
            for (final Operation originalOp : opChain.getOperations()) {
                if (originalOp instanceof OperationChain) {
                    opList.addAll(addOperationsToChain((OperationChain) originalOp, additionalOperations));
                } else {
                    final List<Operation> beforeOps = additionalOperations.getBefore()
                            .get(originalOp.getClass()
                                    .getName());
                    if (null != beforeOps) {
                        opList.addAll(beforeOps);
                    }
                    opList.add(originalOp);
                    final List<Operation> afterOps = additionalOperations.getAfter()
                            .get(originalOp.getClass()
                                    .getName());
                    if (null != afterOps) {
                        opList.addAll(afterOps);
                    }
                }
            }
        }

        return opList;
    }
}
