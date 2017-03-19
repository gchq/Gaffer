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

package uk.gov.gchq.gaffer.data;

import uk.gov.gchq.gaffer.data.element.Element;

/**
 * A <code>IsElementValidator</code> is a simple {@link Validator} to validate if an
 * object is an instance of {@link Element}.
 */
public class IsElementValidator implements Validator<Object> {
    /**
     * @param object the object to check if it is an {@link Element}
     * @return true if the object is an instance of {@link Element}.
     */
    @Override
    public boolean validate(final Object object) {
        return object instanceof Element;
    }
}
