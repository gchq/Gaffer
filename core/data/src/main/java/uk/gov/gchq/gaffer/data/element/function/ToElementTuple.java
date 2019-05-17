/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.data.element.function;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import static java.util.Objects.isNull;

/**
 * A {@code ToElementTuple} is a {@link KorypheFunction} that converts an {@link Element} into an {@link ElementTuple}.
 */
@Since("1.10.0")
@Summary("Converts an Element into an ElementTuple")
public class ToElementTuple extends KorypheFunction<Element, ElementTuple> {

    @Override
    public ElementTuple apply(final Element element) {
        if (isNull(element)) {
            return null;
        }

        return new ElementTuple(element);
    }
}
