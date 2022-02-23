/*
 * Copyright 2017-2021 Crown Copyright
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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

/**
 * A {@code OperationChainHandler} handles OperationChains.
 *
 * @param <O> the output type of the operation chain
 */
public class OperationChainHandler<O> implements OperationHandler<O> {
    public static final String KEY_OPERATIONS = "operations";
    private final OperationChainValidator opChainValidator;
    private final List<OperationChainOptimiser> opChainOptimisers;

    @Override
    public O _doOperation(final Operation operationChain, final Context context, final Store store) throws OperationException {

        final Operation preparedOperationChain = prepareOperationChain(operationChain, context, store);

        Object result = null;
        for (final Operation op : (List<Operation>) preparedOperationChain.get(KEY_OPERATIONS)) {
            updateOperationInput(op, result);
            result = store.handleOperation(op, context);
        }

        return (O) result;
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .fieldRequired(KEY_OPERATIONS, List.class);
    }

    public Operation prepareOperationChain(final Operation operationChain, final Context context, final Store store) {
        final ValidationResult validationResult = opChainValidator.validate(operationChain, context
                .getUser(), store);
        if (!validationResult.isValid()) {
            throw new IllegalArgumentException("Operation chain is invalid. " + validationResult
                    .getErrorString());
        }

        Operation optimisedOperationChain = operationChain;
        for (final OperationChainOptimiser opChainOptimiser : opChainOptimisers) {
            optimisedOperationChain = opChainOptimiser.optimise(optimisedOperationChain);
        }
        return optimisedOperationChain;
    }

    public OperationChainHandler(final OperationChainValidator opChainValidator, final List<OperationChainOptimiser> opChainOptimisers) {
        this.opChainValidator = opChainValidator;
        this.opChainOptimisers = opChainOptimisers;
    }

    protected OperationChainValidator getOpChainValidator() {
        return opChainValidator;
    }

    protected List<OperationChainOptimiser> getOpChainOptimisers() {
        return opChainOptimisers;
    }


    static class Builder extends BuilderSpecificOperation<Builder> {

        public static List<Operation> getOperations(final Operation operation) {
            return (List<Operation>) operation.getOrDefault(KEY_OPERATIONS, new ArrayList());
        }

        public static String toOverviewString(final Operation operation) {
            throw new UnsupportedOperationException("This may need to replaced to StringBuilder");

//            final String opStrings = getOperations(operation).stream()
//                    .filter(o -> null != o)
//                    .map(o -> o.getClass().getSimpleName())
//                    .collect(Collectors.joining("->"));
//
//            return operation.getClass().getSimpleName() + "[" + opStrings + "]";
        }

        public Builder wrap(final Operation operation) {
            requireNonNull(operation);

            List<Operation> operations = (List<Operation>) operation.get(KEY_OPERATIONS);
            if (isNull(operations)) {
                operations = Lists.newArrayList(operation);
            }

            return operations(operations);
        }

        public Builder operations(final List<Operation> operations) {
            operationArg(KEY_OPERATIONS, operations);
            return this;
            //TODO FS Make this a interface Operations object
        }

        @Override
        protected Builder getBuilder() {
            return this;
        }

        @Override
        protected FieldDeclaration getFieldDeclaration() {
            return new OperationChainHandler<>(null, null).getFieldDeclaration();
        }
    }
}
