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

import static uk.gov.gchq.gaffer.data.element.IdentifierType.DIRECTED;

/**
 * A {@code ToCsvHandler} handles {@link ToCsv} operations by applying the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to each item in the
 * input {@link Iterable}.
 */
public class ToCsvHandler implements OutputOperationHandler<ToCsv, Iterable<? extends String>> {
    @Override
    public Iterable<? extends String> doOperation(final ToCsv operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        if (null == operation.getElementGenerator()) {
            throw new IllegalArgumentException("ToCsv operation requires a generator");
        }
        final CsvGenerator elementGenerator = operation.getElementGenerator();
        elementGenerator.addAdditionalFieldsFromSchemaProperties(getPropertiesFromSchema(store.execute(new GetSchema(), context)));

        final Iterable<? extends String> csv = elementGenerator.apply(operation.getInput());
        if (operation.isIncludeHeader()) {
            return new ChainedIterable<>(Collections.singletonList(elementGenerator.getHeader()), csv);
        }

        return csv;
    }

    private LinkedHashMap<String, Class<?>> getPropertiesFromSchema(final Schema schema) {
        final LinkedHashMap<String, Class<?>>  propertiesFromSchema = new LinkedHashMap<>();

        for (final SchemaEntityDefinition schemaEntityDefinition : schema.getEntities().values()) {
            for (final String propertyName : schemaEntityDefinition.getProperties()) {
                final String typeName = schemaEntityDefinition.getPropertyTypeName(propertyName);
                propertiesFromSchema.put(propertyName, schema.getType(typeName).getClazz());
            }
        }

        for (final SchemaEdgeDefinition schemaEdgeDefinition : schema.getEdges().values()) {
            for (final String propertyName : schemaEdgeDefinition.getProperties()) {
                final String typeName = schemaEdgeDefinition.getPropertyTypeName(propertyName);
                propertiesFromSchema.put(propertyName, schema.getType(typeName).getClazz());
            }

            final String directedTypeName = schemaEdgeDefinition.getIdentifierTypeName(DIRECTED);
            propertiesFromSchema.put(DIRECTED.toString(), schema.getType(directedTypeName).getClazz());
        }
        return propertiesFromSchema;
    }

}
