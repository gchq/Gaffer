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
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import java.util.Map;

public class GetSetExportExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetSetExportExample().run();
    }

    public GetSetExportExample() {
        super(GetSetExport.class);
    }

    @Override
    public void runExamples() {
        simpleExportAndGet();
        simpleExportAndGetWithPagination();
        exportMultipleResultsToSetAndGetAllResults();
    }

    public CloseableIterable<?> simpleExportAndGet() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<?>> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new ExportToSet())
                .then(new GetSetExport())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }

    public CloseableIterable<?> simpleExportAndGetWithPagination() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<?>> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new ExportToSet())
                .then(new GetSetExport.Builder()
                        .start(2)
                        .end(4)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }


    public Map<String, CloseableIterable<?>> exportMultipleResultsToSetAndGetAllResults() {
        // ---------------------------------------------------------
        final OperationChain<Map<String, CloseableIterable<?>>> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new ExportToSet.Builder()
                        .key("edges")
                        .build())
                .then(new GetAllEntities())
                .then(new ExportToSet.Builder()
                        .key("entities")
                        .build())
                .then(new GetExports.Builder()
                        .exports(new GetSetExport.Builder()
                                        .key("edges")
                                        .build(),
                                new GetSetExport.Builder()
                                        .key("entities")
                                        .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
