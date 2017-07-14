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
package uk.gov.gchq.gaffer.basic;

import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.store.util.SchemaFactory;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

public class BasicSchema implements SchemaFactory {
    public static final Schema SCHEMA = new Schema.Builder()
            .type("vertex", String.class)
            .type("count", new TypeDefinition.Builder()
                    .clazz(Integer.class)
                    .aggregateFunction(new Sum())
                    .build())
            .type("true", new TypeDefinition.Builder()
                    .clazz(Boolean.class)
                    .validateFunctions(new IsTrue())
                    .build())
            .edge("BasicEdge", new SchemaEdgeDefinition.Builder()
                    .source("vertex")
                    .destination("vertex")
                    .directed("true")
                    .property("count", "count")
                    .build())
            .entity("BasicEntity", new SchemaEntityDefinition.Builder()
                    .vertex("vertex")
                    .property("count", "count")
                    .build())
            .build();

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
}
