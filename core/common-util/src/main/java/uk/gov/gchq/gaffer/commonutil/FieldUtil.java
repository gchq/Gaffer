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

package uk.gov.gchq.gaffer.commonutil;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;

import java.util.function.Predicate;

/**
 * Utility methods for a field
 */
public final class FieldUtil {

    private FieldUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Checks the second value of a {@link Pair} is not null
     *
     * @param fields the fields to validate
     * @return the validation result
     */
    @SafeVarargs
    public static ValidationResult validateRequiredFields(final Pair<String, Object>... fields) {
        final ValidationResult validationResult = new ValidationResult();
        for (final Pair field : fields) {
            if (null == field.getSecond()) {
                validationResult.addError(field.getFirst() + " is required.");
            }
        }
        return validationResult;
    }

    /**
     * Checks that the second value of a {@link Tuple3} satisfies the associated
     * {@link Predicate} condition.
     *
     * @param fields the fields to validate
     * @return the validation result
     */
    @SafeVarargs
    public static ValidationResult validateRequiredFields(final Tuple3<String, Object, Predicate>... fields) {
        final ValidationResult validationResult = new ValidationResult();
        for (final Tuple3<String, Object, Predicate> field : fields) {
            if (!field.get2().test(field.get1())) {
                validationResult.addError(field.get0());
            }
        }
        return validationResult;
    }

}
