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
package uk.gov.gchq.gaffer.store.operation.validator.function;

import uk.gov.gchq.gaffer.operation.impl.function.Function;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

/**
 * A <code>FunctionValidator</code> is a superclass of Validators for Gaffer functions.
 * @param <T>   The function type under validation, eg. Aggregate
 */
public abstract class FunctionValidator<T extends Function> {

    public ValidationResult validate(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null == operation) {
            result.addError("Operation cannot be null.");
        }
        if (null == schema) {
            result.addError("Schema cannot be null.");
        }

        result.add(validateOperation(operation, schema));

        return result;
    }

    /**
     * Should validate the entities and edges, the Element Function, and the Property Classes
     * @param operation The operation to be validated
     * @param schema    The schema to validate with
     * @return          Validation Result of all subsequent validation
     */
    protected abstract ValidationResult validateOperation(final T operation, final Schema schema);

    protected ValidationResult validateEdge(final Map.Entry<String, ?> edgeEntry, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null != edgeEntry) {
            final String group = edgeEntry.getKey();
            final SchemaEdgeDefinition schemaEdgeDefinition = schema.getEdge(group);
            if (null == schemaEdgeDefinition) {
                result.addError("Edge group: " + group + " does not exist in the schema.");
            }
        }
        return result;
    }

    protected ValidationResult validateEntity(final Map.Entry<String, ?> entityEntry, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null != entityEntry) {
            final String group = entityEntry.getKey();
            final SchemaEntityDefinition schemaEntityDefinition = schema.getEntity(group);
            if (null == schemaEntityDefinition) {
                result.addError("Entity group: " + group + " does not exist in the schema.");
            }
        }
        return result;
    }
}
