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

package uk.gov.gchq.gaffer.operation.export;

import uk.gov.gchq.gaffer.operation.io.InputOutput;

/**
 * An {@code ExportTo} is an operation which exports data from a source to a specified
 * output.
 *
 * @param <T> the type of object to export
 */
public interface ExportTo<T> extends
        Export,
        InputOutput<T, T> {
    interface Builder<OP extends ExportTo<T>, T, B extends Builder<OP, T, ?>>
            extends Export.Builder<OP, B>,
            InputOutput.Builder<OP, T, T, B> {
    }
}
