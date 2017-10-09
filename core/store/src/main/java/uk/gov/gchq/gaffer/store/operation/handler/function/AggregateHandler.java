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
package uk.gov.gchq.gaffer.store.operation.handler.function;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.validator.function.AggregateValidator;
import uk.gov.gchq.gaffer.store.operation.validator.function.FunctionValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.HashMap;
import java.util.Map;

public class AggregateHandler implements OutputOperationHandler<Aggregate, Iterable<? extends Element>> {
    private final FunctionValidator<Aggregate> validator = new AggregateValidator();

    @Override
    public Iterable<? extends Element> doOperation(final Aggregate operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Aggregate operation has null iterable of elements");
        }

        // If no entities or edges have been provided then we will assume
        // all elements should be used. This matches the way a View works.
        if (null == operation.getEntities() && null == operation.getEdges()) {
            final Schema schema = store.getSchema();

            final Map<String, AggregatePair> entityMap = new HashMap<>();
            schema.getEntityGroups().forEach(e -> entityMap.put(e, new AggregatePair()));
            operation.setEntities(entityMap);

            final Map<String, AggregatePair> edgeMap = new HashMap<>();
            schema.getEdgeGroups().forEach(e -> edgeMap.put(e, new AggregatePair()));
            operation.setEdges(edgeMap);
        }

        final ValidationResult result = validator.validate(operation, store.getSchema());
        if (!result.isValid()) {
            throw new OperationException("Aggregate operation is invalid. " + result.getErrorString());
        }

        return AggregatorUtil.queryAggregate(operation.getInput(), store.getSchema(), buildView(operation));
    }

    private View buildView(final Aggregate operation) {
        View.Builder builder = new View.Builder();
        if (null != operation.getEntities()) {
            for (final Map.Entry<String, AggregatePair> entry : operation.getEntities().entrySet()) {
                final String group = entry.getKey();
                final AggregatePair pair = entry.getValue();

                builder = builder.entity(group, new ViewElementDefinition.Builder()
                        .groupBy(pair.getGroupBy())
                        .aggregator(pair.getElementAggregator())
                        .build());
            }
        }
        if (null != operation.getEdges()) {
            for (final Map.Entry<String, AggregatePair> entry : operation.getEdges().entrySet()) {
                final String group = entry.getKey();
                final AggregatePair pair = entry.getValue();

                builder = builder.edge(group, new ViewElementDefinition.Builder()
                        .groupBy(pair.getGroupBy())
                        .aggregator(pair.getElementAggregator())
                        .build());
            }
        }
        return builder.build();
    }
}
