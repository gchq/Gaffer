/*
 * Copyright 2016-2017 Crown Copyright
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

/**
 * Utility methods for a field
 */
public final class FieldUtil {
    private FieldUtil() {

    }

    /**
     * Checks the second value of a @Pair is not null
     *
     * @param fields the fields to validate
     * @return a validationResult
     */
    public static ValidationResult validateRequiredFields(final Pair... fields) {
        final ValidationResult validationResult = new ValidationResult();
        for (final Pair field : fields) {
            if (field.getSecond() == null) {
                validationResult.addError(field.getFirst() + " is required.");
            }
        }
        return validationResult;
    }

}
