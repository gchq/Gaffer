/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.example.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;

public class GetAllEdgesExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetAllEdgesExample().run();
    }

    public GetAllEdgesExample() {
        super(GetAllEdges.class);
    }

    @Override
    public void runExamples() {
        getAllEdges();
        getAllEdgesWithCountGreaterThan2();
    }

    public CloseableIterable<Edge> getAllEdges() {
        // ---------------------------------------------------------
        final GetAllEdges operation = new GetAllEdges();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<Edge> getAllEdgesWithCountGreaterThan2() {
        // ---------------------------------------------------------
        final GetAllEdges operation = new GetAllEdges.Builder()
                .view(new View.Builder()
                        .edge("edge", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(2))
                                        .build())
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
