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

import uk.gov.gchq.gaffer.operation.impl.function.Function;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

public class FunctionValidator<T extends Function> {

    public ValidationResult validate(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null == operation) {
            result.addError("Operation cannot be null.");
        }
        if (null == schema) {
            result.addError("Schema cannot be null.");
        }

        result.add(validateEdges(operation, schema));
        result.add(validateEntities(operation, schema));

        return result;
    }

    private ValidationResult validateEntities(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();

        if (null != operation.getEntities()) {
            for (final Map.Entry<String, ?> entry : operation.getEntities().entrySet()) {
                final String group = entry.getKey();
                final SchemaEntityDefinition schemaEntityDefinition = schema.getEntity(group);

                if (null == schemaEntityDefinition) {
                    result.addError("Entity group: " + group + " does not exist in the schema.");
                }
            }
        }

        return result;
    }

    private ValidationResult validateEdges(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();

        if (null != operation.getEdges()) {
            for (final Map.Entry<String, ?> entry : operation.getEdges().entrySet()) {
                final String group = entry.getKey();
                final SchemaEdgeDefinition schemaEdgeDefinition = schema.getEdge(group);

                if (null == schemaEdgeDefinition) {
                    result.addError("Edge group: " + group + " does not exist in the schema.");
                }
            }
        }

        return result;
    }

}
