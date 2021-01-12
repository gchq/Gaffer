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

package uk.gov.gchq.gaffer.integration.extensions;

import uk.gov.gchq.gaffer.integration.template.loader.schemas.SchemaSetup;

import uk.gov.gchq.gaffer.store.StoreProperties;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Provides context for test methods which have a LoaderTestCase parameter.
 */
public class LoaderTestContextProvider extends AbstractContextProvider<LoaderTestCase> {
    @Override
    protected Class<LoaderTestCase> getTestCaseClass() {
        return LoaderTestCase.class;
    }

    @Override
    protected Stream<LoaderTestCase> toTestCases(final StoreProperties storeProperties) {
        return Arrays.stream(SchemaSetup.values())
            .map(schema -> new LoaderTestCase(storeProperties, schema));
    }
}
