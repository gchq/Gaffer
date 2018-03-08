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

package uk.gov.gchq.gaffer.operation;

/**
 * A {@code Validatable} operation defines an operation with an iterable of {@link uk.gov.gchq.gaffer.data.element.Element}s
 * that can optionally be validated before being processed.
 */
public interface Validatable {
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

    interface Builder<OP extends Validatable, B extends Builder<OP, ?>> extends Operation.Builder<OP, B> {
        /**
         * @param skipInvalidElements the skipInvalidElements flag to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setSkipInvalidElements(boolean)
         */
        default B skipInvalidElements(final boolean skipInvalidElements) {
            _getOp().setSkipInvalidElements(skipInvalidElements);
            return _self();
        }

        /**
         * @param validate the validate flag to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setValidate(boolean)
         */
        default B validate(final boolean validate) {
            _getOp().setValidate(validate);
            return _self();
        }
    }
}
