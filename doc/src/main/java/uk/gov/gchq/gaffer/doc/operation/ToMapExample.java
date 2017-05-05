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
package uk.gov.gchq.gaffer.doc.operation;

import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import java.util.Map;

public class ToMapExample extends OperationExample {
    public ToMapExample() {
        super(ToMap.class);
    }

    public static void main(final String[] args) throws OperationException {
        new ToMapExample().run();
    }

    @Override
    public void runExamples() {
        toMapExample();
    }

    public Iterable<? extends Map<String, Object>> toMapExample() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Map<String, Object>>> opChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .build())
                .then(new ToMap.Builder()
                        .generator(new MapGenerator.Builder()
                                .group("group")
                                .vertex("vertex")
                                .source("source")
                                .property("count", "total count")
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
