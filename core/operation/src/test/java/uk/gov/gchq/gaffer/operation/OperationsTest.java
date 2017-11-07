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
package uk.gov.gchq.gaffer.operation;

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;

/**
 * Provides a common interface for testing implementations of the {@link Operations} class.
 * @param <T>   The implementation of {@link Operations} to be tested
 */
public abstract class OperationsTest<T extends Operation> extends JSONSerialisationTest<T> {

    /**
     * Should return a {@link java.util.Collection} of {@link Operation}s that the
     * {@link Operations} T contains.
     */
    @Test
    public abstract void shouldGetOperations();

}
