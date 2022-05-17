/*
 * Copyright 2016-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.integration;

import org.junit.jupiter.api.Test;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Runs the full suite of gaffer store integration tests. To run the tests against
 * a specific store, simply extend this class - you don't need to annotate
 * your class.
 */

@Suite
@SelectPackages("uk.gov.gchq.gaffer.integration.impl")
@IncludeClassNamePatterns(".*IT")
public abstract class AbstractStoreITs {
    private final Map<Class<? extends AbstractStoreIT>, Map<String, String>> skipTestMethods;

    public AbstractStoreITs(final StoreProperties storeProperties, final Schema schema) {
        this.skipTestMethods = AbstractStoreIT.getSkipTestMethods();
        AbstractStoreIT.setStoreSchema(schema);
        AbstractStoreIT.setStoreProperties(storeProperties);
        System.out.println(storeProperties);
        System.out.println(schema);
    }

    public AbstractStoreITs(final StoreProperties storeProperties) {
        this(storeProperties, new Schema());
    }

    protected void skipTestMethod(final Class<? extends AbstractStoreIT> testClass, final String methodToSkip, final String justification) {
        HashMap<String, String> methodJustificationMap = new HashMap<>();
        methodJustificationMap.put(methodToSkip, justification);
        skipTestMethods.put(testClass, methodJustificationMap);
    }

    @Test
    void emptyTest() {
        //Required for JUnit 5. Without any test methods the constructor is not called and the Suite is not run.
    }
}
