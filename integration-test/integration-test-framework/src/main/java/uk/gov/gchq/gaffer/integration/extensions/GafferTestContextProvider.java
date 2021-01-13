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

import uk.gov.gchq.gaffer.store.StoreProperties;

import java.util.stream.Stream;

/**
 * Provides context for tests with a GafferTestCaseParameter
 */
public class GafferTestContextProvider extends AbstractContextProvider<GafferTestCase> {

    @Override
    protected Class<GafferTestCase> getTestCaseClass() {
        return GafferTestCase.class;
    }

    @Override
    protected Stream<GafferTestCase> toTestCases(final StoreProperties storeProperties) {
        return Stream.of(new GafferTestCase(storeProperties));
    }
}
