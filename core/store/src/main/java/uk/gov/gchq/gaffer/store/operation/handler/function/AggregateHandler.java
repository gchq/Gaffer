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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.function.Aggregate;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;

import java.util.Map;

public class AggregateHandler implements OutputOperationHandler<Aggregate, Iterable<? extends Element>> {
    @Override
    public Iterable<? extends Element> doOperation(final Aggregate operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Aggregate operation has null iterable of elements");
        }

        return AggregatorUtil.queryAggregate(operation.getInput(), store.getSchema(), buildView(operation));
    }

    private View buildView(final Aggregate operation) {
        View.Builder builder = new View.Builder();
        if (null != operation.getEntities()) {
            for (Map.Entry<String, Pair<String[], ElementAggregator>> entry : operation.getEntities().entrySet()) {
                final String group = entry.getKey();
                final Pair<String[], ElementAggregator> pair = entry.getValue();

                builder = builder.entity(group, new ViewElementDefinition.Builder()
                        .groupBy(pair.getFirst())
                        .aggregator(pair.getSecond())
                        .build());
            }
        }
        if (null != operation.getEdges()) {
            for (Map.Entry<String, Pair<String[], ElementAggregator>> entry : operation.getEdges().entrySet()) {
                final String group = entry.getKey();
                final Pair<String[], ElementAggregator> pair = entry.getValue();

                builder = builder.edge(group, new ViewElementDefinition.Builder()
                        .groupBy(pair.getFirst())
                        .aggregator(pair.getSecond())
                        .build());
            }
        }
        return builder.build();
    }
}
