/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.core.exception;

/**
 * Subtype of {@link RuntimeException} that wraps an {@link Error}.
 */
public class GafferWrappedErrorRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 5497060484135689800L;
    private Error error;

    public GafferWrappedErrorRuntimeException(final Error error) {
        super(error.getSimpleMessage());
        this.error = error;
    }

    public Error getError() {
        return error;
    }
}
