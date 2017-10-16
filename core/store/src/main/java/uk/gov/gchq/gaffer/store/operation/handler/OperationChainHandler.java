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
package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;

/**
 * A {@code OperationChainHandler} handles {@link OperationChain}s.
 *
 * @param <OUT> the output type of the operation chain
 */
public class OperationChainHandler<OUT> implements OutputOperationHandler<OperationChain<OUT>, OUT> {
    private final OperationChainValidator opChainValidator;
    private final List<OperationChainOptimiser> opChainOptimisers;

    @Override
    public OUT doOperation(final OperationChain<OUT> operationChain, final Context context, final Store store) throws OperationException {

        prepareOperationChain(operationChain, context, store);

        Object result = null;
        for (final Operation op : operationChain.getOperations()) {
            updateOperationInput(op, result);
            result = store.handleOperation(op, context);
        }

        return (OUT) result;
    }

    private <O> OperationChain<O> prepareOperationChain(final OperationChain<O> operationChain, final Context context, final Store store) {
        final ValidationResult validationResult = opChainValidator.validate(operationChain, context
                .getUser(), store);
        if (!validationResult.isValid()) {
            throw new IllegalArgumentException("Operation chain is invalid. " + validationResult
                    .getErrorString());
        }

        OperationChain<O> optimisedOperationChain = operationChain;
        for (final OperationChainOptimiser opChainOptimiser : opChainOptimisers) {
            optimisedOperationChain = opChainOptimiser.optimise(optimisedOperationChain);
        }
        return optimisedOperationChain;
    }

    protected void updateOperationInput(final Operation op, final Object result) {
        if (null != result) {
            if (op instanceof OperationChain) {
                if (!((OperationChain) op).getOperations().isEmpty()) {
                    final Operation firstOp = (Operation) ((OperationChain) op).getOperations()
                            .get(0);
                    if (firstOp instanceof Input) {
                        setOperationInput(firstOp, result);
                    }
                }
            } else if (op instanceof Input) {
                setOperationInput(op, result);
            }
        }
    }

    public OperationChainHandler(final OperationChainValidator opChainValidator, final List<OperationChainOptimiser> opChainOptimisers) {
        this.opChainValidator = opChainValidator;
        this.opChainOptimisers = opChainOptimisers;
    }

    private void setOperationInput(final Operation op, final Object result) {
        if (null == ((Input) op).getInput()) {
            ((Input) op).setInput(result);
        }
    }
}
