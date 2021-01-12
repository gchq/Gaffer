/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.schema;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Set;

/**
 * An {@code ViewValidator} validates a view against a {@link Schema}
 * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition}.
 * Checks all function input and output types are compatible with the
 * properties and identifiers in the Schema and the transient properties in the
 * View.
 */
@Deprecated
public class ViewValidator {
    public ValidationResult validate(final View view, final Schema schema, final Set<StoreTrait> storeTraits) {
        return new ValidationResult();
    }

}
