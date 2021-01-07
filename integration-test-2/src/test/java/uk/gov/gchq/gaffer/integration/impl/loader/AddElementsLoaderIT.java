/*
 * Copyright 2020 Crown Copyright
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

import org.springframework.beans.factory.annotation.Autowired;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.MultiStoreTest;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestCase;
import uk.gov.gchq.gaffer.integration.factory.MapStoreGraphFactory;
import uk.gov.gchq.gaffer.integration.template.loader.AddElementsLoaderITTemplate;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

@MultiStoreTest
public class AddElementsLoaderIT extends AddElementsLoaderITTemplate {

    @Autowired
    private GraphFactory graphFactory;

    private void resetRemoteGraph(final Schema schema) {
        if (graphFactory instanceof MapStoreGraphFactory) {
            ((MapStoreGraphFactory) graphFactory).reset(schema);
        } else {
            throw new RuntimeException("Expected the MapStoreGraph Factory to be injected");
        }
    }

    @Override
    protected void beforeEveryTest(final LoaderTestCase testCase) throws Exception {
        resetRemoteGraph(testCase.getSchemaSetup().getTestSchema().getSchema());
        super.beforeEveryTest(testCase);
    }

    @Override
    @GafferTest(excludeStores = {ParquetStore.class, FederatedStore.class}) // todo find out why these fail
    public void shouldThrowExceptionWithUsefulMessageWhenInvalidElementsAdded(final LoaderTestCase testCase) throws OperationException {
        super.shouldThrowExceptionWithUsefulMessageWhenInvalidElementsAdded(testCase);
    }
}
