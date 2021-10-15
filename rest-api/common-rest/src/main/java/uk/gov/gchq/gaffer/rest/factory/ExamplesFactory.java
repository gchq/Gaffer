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

package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.operation.Operation;

/**
 * An ExampleFactory creates examples of Operations
 */
public interface ExamplesFactory {
    /**
     * Generates an example for any {@link Operation} class.
     *
     * @param opClass the operation to create an example for
     * @return the example class
     * @throws IllegalAccessException if the operation could not be created
     * @throws InstantiationException if the operation could not be created
     */
    Operation generateExample(final Class<? extends Operation> opClass) throws IllegalAccessException, InstantiationException;
}
