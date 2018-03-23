/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.iterable;

import uk.gov.gchq.koryphe.ValidationResult;

/**
 * An {@code Validator} validates objects of type T and returns true if they are valid.
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

    /**
     * <p>
     * Validates the given object and results a ValidationResult that can
     * contain information as to why validation fails.
     * </p>
     * <p>
     * It is less efficient than just calling validate as complex validation
     * strings may be built to detail why objects are invalid.
     * </p>
     *
     * @param obj an object of type T to validate.
     * @return the ValidationResult.
     */
    default ValidationResult validateWithValidationResult(final T obj) {
        final boolean result = validate(obj);
        final ValidationResult validationResult = new ValidationResult();
        if (!result) {
            validationResult.addError("Validation failed for obj: " + obj);
        }

        return validationResult;
    }
}
