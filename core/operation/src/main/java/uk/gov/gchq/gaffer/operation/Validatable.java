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

package uk.gov.gchq.gaffer.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;

/**
 * A <code>Validatable</code> operation defines an operation with an iterable of {@link uk.gov.gchq.gaffer.data.element.Element}s
 * that can optionally be validated before being processed.
 * <p>
 *
 * @param <OUTPUT> the output type
 */
public interface Validatable<OUTPUT> extends Operation<CloseableIterable<Element>, OUTPUT> {
    /**
     * @return true if invalid elements should be skipped. Otherwise false if the operation should fail.
     */
    boolean isSkipInvalidElements();

    /**
     * @param skipInvalidElements true if invalid elements should be skipped. Otherwise false if the operation should fail.
     */
    void setSkipInvalidElements(final boolean skipInvalidElements);

    /**
     * @return true if the operation should be validated. Otherwise false.
     */
    boolean isValidate();


    /**
     * @param validate true if the operation should be validated. Otherwise false.
     */
    void setValidate(final boolean validate);

    /**
     * @return the input {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s in the operation.
     */
    CloseableIterable<Element> getElements();

    /**
     * @param elements the {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be set as the
     *                 input for the operation.
     */
    void setElements(final CloseableIterable<Element> elements);
}
