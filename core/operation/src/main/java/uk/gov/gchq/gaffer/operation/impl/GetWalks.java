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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@code GetWalks} class is used to retrieve all of the walks in a graph
 * starting from one of a set of provided {@link EntityId}s, with a maximum
 * length.
 * <p>
 * A GetWalks operation is configured using a user-supplied list of {@link
 * GetElements} operations. These are executed sequentially, with the output of
 * one operation providing the input {@link EntityId}s for the next.
 */
@JsonPropertyOrder(value = {"class", "input", "operations"}, alphabetic = true)
@Since("1.1.0")
@Summary("Walks around the Graph, returning the full walks taken")
public class GetWalks implements
        InputOutput<Iterable<? extends EntityId>, Iterable<Walk>>,
        MultiEntityIdInput,
        Operations<OperationChain<Iterable<Element>>> {

    public static final String HOP_DEFINITION = "A hop is a GetElements operation that selects at least 1 edge group.";
    public static final int DEFAULT_RESULTS_LIMIT = 1000000;

    private List<OperationChain<Iterable<Element>>> operations = new ArrayList<>();
    private Iterable<? extends EntityId> input;
    private Map<String, String> options;
    private Integer resultsLimit = DEFAULT_RESULTS_LIMIT;

    @Override
    public Iterable<? extends EntityId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends EntityId> input) {
        this.input = input;
    }

    @Override
    public List<OperationChain<Iterable<Element>>> getOperations() {
        return operations;
    }

    public void setOperations(final List<Output<Iterable<Element>>> operations) {
        this.operations.clear();
        addOperations(operations);
    }

    public void addOperations(final List<Output<Iterable<Element>>> operations) {
        operations.forEach(op -> this.operations.add(OperationChain.wrap(op)));
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = InputOutput.super.validate();

        final int getEdgeOperations = getNumberOfGetEdgeOperations();

        if (getEdgeOperations < 1) {
            result.addError("No hops were provided. " + HOP_DEFINITION);
        } else {
            int i = 0;
            for (final OperationChain<Iterable<Element>> operation : operations) {
                if (operation.getOperations().isEmpty()) {
                    result.addError("Operation chain " + i + " contains no operations");
                } else {
                    final Operation firstOp = operation.getOperations().get(0);
                    if (firstOp instanceof Input) {
                        if (null != ((Input) firstOp).getInput()) {
                            result.addError("The input for operations must be null.");
                        }
                    } else {
                        result.addError("The first operation in operation chain " + i + ": " + firstOp.getClass().getName() + " is not be able to accept the input seeds. It must implement " + Input.class.getName());
                    }
                }

                if (getNumberOfGetEdgeOperationsWithoutRepeats(operation) < 1 && i < (operations.size() - 1)) {
                    // An operation does not contain a hop
                    result.addError("All operations must contain a single hop. Operation " + i + " does not contain a hop. The only exception is the last operation, which is allowed to just fetch Entities. " + HOP_DEFINITION);
                } else if (getNumberOfGetEdgeOperationsWithoutRepeats(operation) > 1) {
                    // An operation does not contain a hop
                    result.addError("All operations must contain a single hop. Operation " + i + " contains multiple hops.");
                }

                i++;
            }
        }

        return result;
    }

    @JsonIgnore
    public int getNumberOfGetEdgeOperations() {
        return getNumberOfGetEdgeOperations(operations);
    }

    private int getNumberOfGetEdgeOperations(final Operation op) {
        int hops = 0;
        if (op instanceof While) {
            hops += (((While) op).getMaxRepeats() * getNumberOfGetEdgeOperations(((While) op).getOperation()));
        } else if (op instanceof Operations) {
            hops += getNumberOfGetEdgeOperations(((Operations<?>) op).getOperations());
        } else if (op instanceof GetElements) {
            final GetElements getElements = (GetElements) op;
            if (null != getElements.getView() && getElements.getView().hasEdges()) {
                hops += 1;
            }
        }
        return hops;
    }

    private int getNumberOfGetEdgeOperations(final Iterable<? extends Operation> ops) {
        return Streams.toStream(ops)
                .mapToInt(this::getNumberOfGetEdgeOperations)
                .sum();
    }

    private int getNumberOfGetEdgeOperationsWithoutRepeats(final Operation op) {
        int hops = 0;
        if (op instanceof While) {
            hops += getNumberOfGetEdgeOperationsWithoutRepeats(((While) op).getOperation());
        } else if (op instanceof Operations) {
            hops += getNumberOfGetEdgeOperationsWithoutRepeats(((Operations<?>) op).getOperations());
        } else if (op instanceof GetElements) {
            final GetElements getElements = (GetElements) op;
            if (null != getElements.getView() && getElements.getView().hasEdges()) {
                hops += 1;
            }
        }
        return hops;
    }

    private int getNumberOfGetEdgeOperationsWithoutRepeats(final Iterable<? extends Operation> ops) {
        return Streams.toStream(ops)
                .mapToInt(this::getNumberOfGetEdgeOperationsWithoutRepeats)
                .sum();
    }

    @Override
    public TypeReference<Iterable<Walk>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableWalk();
    }

    @Override
    public GetWalks shallowClone() {
        List clonedOps = operations.stream().map(Output::shallowClone).collect(Collectors.toList());
        return new GetWalks.Builder()
                .input(input)
                .operations(clonedOps)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public Integer getResultsLimit() {
        return resultsLimit;
    }

    public void setResultsLimit(final Integer resultsLimit) {
        this.resultsLimit = resultsLimit;
    }

    public static final class Builder
            extends Operation.BaseBuilder<GetWalks, Builder>
            implements InputOutput.Builder<GetWalks, Iterable<? extends EntityId>, Iterable<Walk>, Builder>,
            MultiEntityIdInput.Builder<GetWalks, Builder> {

        public Builder() {
            super(new GetWalks());
        }

        public Builder operations(final Output... operations) {
            if (null != operations) {
                _getOp().setOperations(Lists.newArrayList(operations));
            }
            return _self();
        }

        public Builder operations(final List<Output<Iterable<Element>>> operations) {
            if (null != operations) {
                _getOp().setOperations(operations);
            }
            return _self();
        }

        /**
         * Adds an operation.
         *
         * @param operation the operation to add
         * @return the Builder
         * @deprecated use addOperations instead
         */
        @Deprecated
        public Builder operation(final Output operation) {
            return addOperations(operation);
        }

        public Builder addOperations(final Output... operations) {
            if (null != operations) {
                _getOp().addOperations(Lists.newArrayList(operations));
            }
            return _self();
        }

        public Builder addOperations(final List<Output<Iterable<Element>>> operations) {
            if (null != operations) {
                _getOp().addOperations(operations);
            }
            return _self();
        }

        public Builder resultsLimit(final Integer resultLimit) {
            _getOp().setResultsLimit(resultLimit);
            return _self();
        }
    }
}
