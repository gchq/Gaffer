/*
 * Copyright 2017-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.rest.service.v2.example;

import uk.gov.gchq.gaffer.rest.factory.AbstractExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.inject.Inject;

/**
 * Default implementation of the {@link uk.gov.gchq.gaffer.rest.service.v2.example.ExamplesFactory}
 * interface. Required to be registered with HK2 to allow the correct {@link
 * uk.gov.gchq.gaffer.rest.factory.GraphFactory} object to be injected.
 */
public class DefaultExamplesFactory extends AbstractExamplesFactory {

    @Inject
    private GraphFactory graphFactory;

    @Override
    protected Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }
}
