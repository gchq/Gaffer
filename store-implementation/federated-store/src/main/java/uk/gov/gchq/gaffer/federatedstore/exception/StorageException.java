/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.exception;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;

/**
 * An {@code StorageException} should be thrown when a problem occurs accessing
 * the federated graph storage.
 */
public class StorageException extends GafferCheckedException {
    private static final long serialVersionUID = -1391854996347271983L;

    public StorageException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new storage exception with the specified detail message.
     *
     * @param message Storage exception detail message.
     */
    public StorageException(final String message) {
        super(message);
    }

    /**
     * Constructs a new storage exception with the specified detail message and cause.
     *
     * @param message Storage exception detail message.
     * @param cause   Storage exception cause.
     */
    public StorageException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
