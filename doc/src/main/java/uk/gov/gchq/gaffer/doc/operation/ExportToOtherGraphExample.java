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

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.io.IOException;

public class ExportToOtherGraphExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new ExportToOtherGraphExample().run();
    }

    public ExportToOtherGraphExample() {
        super(ExportToSet.class);
    }

    @Override
    public void runExamples() {
        simpleExport();
    }

    public Iterable<?> simpleExport() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> opChain;
        StoreProperties storeProperties = new StoreProperties();
        try {
            storeProperties.getProperties().load(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new ExportToOtherGraph.Builder<CloseableIterable<? extends Element>>()
                        .storeProperties(storeProperties)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }
}
