/*
 * Copyright 2017-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.data.generator.CsvFormat;
import uk.gov.gchq.gaffer.data.generator.CsvGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.iterable.ChainedIterable;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * A {@code ToCsvHandler} handles {@link ToCsv} operations by applying the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to each item in the
 * input {@link Iterable}.
 */
public class ToCsvHandler implements OutputOperationHandler<ToCsv, Iterable<? extends String>> {
    @Override
    public Iterable<? extends String> doOperation(final ToCsv operation, final Context context, final Store store) throws OperationException {
        CsvGenerator csvGenerator;
        final Schema schema;
        if (operation.getInput() == null) {
            return null;
        }

        if (operation.getElementGenerator() == null && operation.getCsvFormat() == null) {
            throw new IllegalArgumentException("ToCsv operation requires a generator, supply one or provide a CsvFormat");
        } else if (operation.getElementGenerator() != null && operation.getCsvFormat() != null) {
            throw new IllegalArgumentException("ToCsv operation requires either a generator or a CsvFormat not both");
        } else if (operation.getElementGenerator() == null && operation.getCsvFormat() != null) {
            schema = store.execute(new GetSchema(), context);

            csvGenerator = createGenerator(operation.getCsvFormat(), getPropertyHeadersFromSchema(schema));
        } else {
            csvGenerator = operation.getElementGenerator();
        }

        final Iterable<? extends String> csv = csvGenerator.apply(operation.getInput());
        if (operation.isIncludeHeader()) {
            return new ChainedIterable<>(Collections.singletonList(csvGenerator.getHeader()), csv);
        }

        return csv;
    }

    private LinkedHashMap<String, String> getPropertyHeadersFromSchema(final Schema schema) throws OperationException {
        LinkedHashMap<String, String> propertyHeadersFromSchema = new LinkedHashMap<>();
        for (final SchemaEntityDefinition schemaEntityDefinition : schema.getEntities().values()) {
            for (final String propertyName:schemaEntityDefinition.getProperties()) {
                String typeName = schemaEntityDefinition.getPropertyTypeName(propertyName);
                propertyHeadersFromSchema.put(propertyName, schema.getType(typeName).getClazz().getSimpleName());
            }
        }
        for (final SchemaEdgeDefinition schemaEdgeDefinition : schema.getEdges().values()) {
            for (final IdentifierType identifierType:schemaEdgeDefinition.getIdentifiers()) {
                if (identifierType.toString().equals(identifierType.DIRECTED.toString())) {
                    String typeName = schemaEdgeDefinition.getIdentifierTypeName(identifierType);
                    propertyHeadersFromSchema.put(identifierType.toString(), schema.getType(typeName).getClazz().getSimpleName());
                }
            }
            for (final String propertyName:schemaEdgeDefinition.getProperties()) {
                String typeName = schemaEdgeDefinition.getPropertyTypeName(propertyName);
                propertyHeadersFromSchema.put(propertyName, schema.getType(typeName).getClazz().getSimpleName());
            }
        }
        return propertyHeadersFromSchema;
    }


    private CsvGenerator createGenerator(final CsvFormat csvFormat, final LinkedHashMap<String, String> propertyHeadersFromSchema) {
        return new CsvGenerator.Builder()
                .identifiersFromFormat(CsvFormat.getIdentifiers(csvFormat))
                .propertyHeadersFromSchema(propertyHeadersFromSchema)
                .build();
    }

}
