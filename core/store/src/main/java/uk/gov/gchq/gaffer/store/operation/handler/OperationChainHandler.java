/*
 * Copyright 2017-2024 Crown Copyright
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

import uk.gov.gchq.gaffer.commonutil.otel.OtelUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

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

        final OperationChain<OUT> preparedOperationChain = prepareOperationChain(operationChain, context, store);

        Object result = null;
        for (final Operation op : preparedOperationChain.getOperations()) {
            // OpenTelemetry hooks
            Span span = OtelUtil.startSpan(this.getClass().getName(), op.getClass().getName());
            span.setAttribute(OtelUtil.JOB_ID_ATTRIBUTE, context.getJobId());
            if (op instanceof OperationView && ((OperationView) op).getView() != null) {
                span.setAttribute(OtelUtil.VIEW_ATTRIBUTE, ((OperationView) op).getView().toString());
            }

            // Sets the span to current so parent child spans are auto linked
            try (Scope scope = span.makeCurrent()) {
                updateOperationInput(op, result);
                result = store.handleOperation(op, context);
            } finally {
                span.end();
            }
        }

        return (OUT) result;
    }

    public <O> OperationChain<O> prepareOperationChain(final OperationChain<O> operationChain, final Context context, final Store store) {
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
}
