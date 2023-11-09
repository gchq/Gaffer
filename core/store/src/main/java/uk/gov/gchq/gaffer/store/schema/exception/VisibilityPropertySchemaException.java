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
import uk.gov.gchq.gaffer.store.schema.Schema;

public class VisibilityPropertySchemaException extends SchemaException {
    public static final String VISIBILITY_PROPERTY = "visibility property";
    private static final String SCHEMAS_CONFLICT_WITH_VISIBILITY_PROPERTY = String.format(Schema.FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S, VISIBILITY_PROPERTY);

    public VisibilityPropertySchemaException(final String message, final Throwable e) {
        super(message, e);
    }

    public VisibilityPropertySchemaException(final String thisVisibilityProperty, final String thatVisibilityProperty) {
        super(String.format(Schema.FORMAT_EXCEPTION, SCHEMAS_CONFLICT_WITH_VISIBILITY_PROPERTY, thisVisibilityProperty, thatVisibilityProperty));
    }
}
