/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

public class SplitElementGroupDefSchemaException extends SchemaException {

    private static final String ELEMENT_GROUP_MUST_ALL_BE_DEFINED_IN_A_SINGLE_SCHEMA = "Element group properties cannot be defined in different schema parts, they must all be defined in a single schema part. Please fix this group: ";

    public SplitElementGroupDefSchemaException(final String sharedGroup) {
        super(ELEMENT_GROUP_MUST_ALL_BE_DEFINED_IN_A_SINGLE_SCHEMA + sharedGroup);
    }

    public SplitElementGroupDefSchemaException(final String message, final Throwable e) {
        super(message, e);
    }
}
