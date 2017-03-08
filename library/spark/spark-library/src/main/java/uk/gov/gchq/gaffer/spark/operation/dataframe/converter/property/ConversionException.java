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
package uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property;

import java.io.IOException;

/**
 * An {@link IOException} used in a <code>Converter</code> class.
 */
public class ConversionException extends IOException {

    private static final long serialVersionUID = -524296061365482027L;

    public ConversionException(final String message) {
        super(message);
    }

    public ConversionException(final String message, final Throwable e) {
        super(message, e);
    }

}
