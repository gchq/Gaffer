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
package uk.gov.gchq.gaffer.data.element.function;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * An {@code ExtractGroup} is a {@link KorypheFunction} for
 * extracting a group from an {@link Element}.
 * If the Element is null, this function will return null.
 */
@Since("1.4.0")
@Summary("Extracts a group from an element")
public class ExtractGroup extends KorypheFunction<Element, String> {
    @Override
    public String apply(final Element element) {
        return null != element ? element.getGroup() : null;
    }
}
