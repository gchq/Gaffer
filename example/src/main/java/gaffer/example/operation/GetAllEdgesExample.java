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
package gaffer.example.operation;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.operation.OperationException;
import gaffer.operation.impl.get.GetAllEdges;

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
        final String opJava = "new GetAllEdges();";
        return runExample(new GetAllEdges(), opJava);
    }

    public CloseableIterable<Edge> getAllEdgesWithCountGreaterThan2() {
        final String opJava = "new GetAllEdges.Builder()\n"
                + "                .view(new View.Builder()\n"
                + "                        .edge(\"edge\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(2))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .build())\n"
                + "                .build();";
        return runExample(new GetAllEdges.Builder()
                .view(new View.Builder()
                        .edge("edge", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(2))
                                        .build())
                                .build())
                        .build())
                .build(), opJava);
    }
}
