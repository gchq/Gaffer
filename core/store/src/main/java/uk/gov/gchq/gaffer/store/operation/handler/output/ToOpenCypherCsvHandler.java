/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.generator.OpenCypherCsvGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToOpenCypherCsv;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.iterable.ChainedIterable;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * A {@code ToCsvHandler} handles {@link ToOpenCypherCsv} operations by applying the generated
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator}
 * to each item in the input {@link Iterable}.
 */
public class ToOpenCypherCsvHandler implements OutputOperationHandler<ToOpenCypherCsv, Iterable<? extends String>> {
    @Override
    public Iterable<? extends String> doOperation(final ToOpenCypherCsv operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        OpenCypherCsvGenerator openCypherCsvGenerator = createGenerator(getHeadersFromSchema(store), (ToOpenCypherCsv) operation);

        final Iterable<? extends String> csv = openCypherCsvGenerator.apply(operation.getInput());
        return new ChainedIterable<>(Collections.singletonList(openCypherCsvGenerator.getHeader()), csv);

    }

    LinkedHashMap<String, String> getHeadersFromSchema(final Store store) {
        Schema schema = store.getSchema();
        LinkedHashMap<String, String> headersFromSchema = new LinkedHashMap<>();
        for (final SchemaEntityDefinition schemaEntityDefinition : schema.getEntities().values()) {
            for (final IdentifierType identifierType:schemaEntityDefinition.getIdentifiers()) {
                if (identifierType.toString() == "DIRECTED") {
                    headersFromSchema.put(identifierType.toString(), schemaEntityDefinition.getIdentifierTypeName(identifierType));
                }
            }
            for (final String propertyName:schemaEntityDefinition.getProperties()) {
                headersFromSchema.put(propertyName, schemaEntityDefinition.getPropertyTypeName(propertyName));
            }
        }
        for (final SchemaEdgeDefinition schemaEdgeDefinition : schema.getEdges().values()) {
            for (final IdentifierType identifierType:schemaEdgeDefinition.getIdentifiers()) {
                if (identifierType.toString() == "DIRECTED") {
                    headersFromSchema.put(identifierType.toString(), schemaEdgeDefinition.getIdentifierTypeName(identifierType));
                }
            }
            for (final String propertyName:schemaEdgeDefinition.getProperties()) {
                headersFromSchema.put(propertyName, schemaEdgeDefinition.getPropertyTypeName(propertyName));
            }
        }
        return headersFromSchema;
    }

     private OpenCypherCsvGenerator createGenerator(final LinkedHashMap<String, String> headersFromSchema, final boolean neo4jFormat) {
        return new OpenCypherCsvGenerator.Builder()
                .headers(headersFromSchema)
                .neo4jFormat(neo4jFormat)
                .build();
    }
    private OpenCypherCsvGenerator createGenerator(final LinkedHashMap<String, String> headersFromSchema, final ToOpenCypherCsv operation) {
        return createGenerator(headersFromSchema, operation.isNeo4jFormat());
    }
}
