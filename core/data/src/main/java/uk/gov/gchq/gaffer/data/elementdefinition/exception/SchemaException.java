/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.exception;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;

import static uk.gov.gchq.gaffer.core.exception.Status.BAD_REQUEST;

/**
 * An {@code SchemaException} is thrown when a schema is found to be invalid.
 * This can occur for several reasons including when deserialisation of a json schema fails
 * or a schema has invalid attributes.
 */
public class SchemaException extends GafferRuntimeException {
    private static final long serialVersionUID = 3150434301320173603L;
    @SuppressWarnings("PMD.AvoidStringBufferField")
    private StringBuilder prependToMessage;

    public SchemaException(final String message) {
        super(message, BAD_REQUEST);
    }

    public SchemaException(final String message, final Throwable e) {
        super(message, e, BAD_REQUEST);
    }

    public SchemaException prependToMessage(final String prependToMessage) {
        if (null == this.prependToMessage) {
            this.prependToMessage = new StringBuilder();
        }
        //preAppend
        this.prependToMessage.insert(0, prependToMessage);

        return this;
    }

    @Override
    public String getMessage() {
        return (null == prependToMessage) ? super.getMessage() : prependToMessage.append(super.getMessage()).toString();
    }
}
