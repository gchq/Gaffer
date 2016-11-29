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

package uk.gov.gchq.gaffer.data;

/**
 * An <code>Validator</code> validates objects of type T and returns true if they are valid.
 *
 * @param <T> the type of object the validator will validate.
 */
public interface Validator<T> {
    /**
     * Validates the given object.
     *
     * @param obj an object of type T to validate.
     * @return true if the provided object is valid.
     */
    boolean validate(final T obj);
}
