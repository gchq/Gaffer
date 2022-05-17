/*
 * Copyright 2018-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl.loader;

import org.junit.jupiter.api.Nested;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.AggregationSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.BasicSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.FullSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.VisibilitySchemaLoader;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;

import static uk.gov.gchq.gaffer.store.schema.TestSchema.*;

/**
 * {@link UserLoaderIT} implementation for the {@link AddElements} operation.
 * This class defines and runs a set of nested tests for each different type of {@link uk.gov.gchq.gaffer.store.schema.TestSchema}.
 */
public class AddElementsLoaderIT {
    private class AddElementsLoader extends UserLoaderIT {
        @Override
        protected void addElements(final Iterable<? extends Element> input) throws OperationException {
            graph.execute(new AddElements.Builder()
                    .input(input)
                    .build(), getUser());
        }
    }
    @Nested
    public class FullSchemaAddElementsLoaderIT extends AddElementsLoader {
        public FullSchemaAddElementsLoaderIT() {
            super();
            this.schema = FULL_SCHEMA.getSchema();
            this.loader = new FullSchemaLoader();
        }
    }
    @Nested
    public class VisibilitySchemaAddElementsLoaderIT extends AddElementsLoader {
        public VisibilitySchemaAddElementsLoaderIT() {
            super();
            this.schema = VISIBILITY_SCHEMA.getSchema();
            this.loader = new VisibilitySchemaLoader();
        }
    }

    @Nested
    public class AggregationSchemaAddElementsLoaderIT extends AddElementsLoader {
        public AggregationSchemaAddElementsLoaderIT() {
            super();
            this.schema = AGGREGATION_SCHEMA.getSchema();
            this.loader = new AggregationSchemaLoader();
        }
    }

    @Nested
    public class BasicSchemaAddElementsLoaderIT extends AddElementsLoader {
        public BasicSchemaAddElementsLoaderIT() {
            super();
            this.schema = BASIC_SCHEMA.getSchema();
            this.loader = new BasicSchemaLoader();
        }
    }
}
