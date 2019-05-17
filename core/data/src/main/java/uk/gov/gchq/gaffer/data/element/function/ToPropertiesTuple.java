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

import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import static java.util.Objects.isNull;

/**
 * A {@code ToPropertiesTuple} is a {@link KorypheFunction} that converts Element {@link Properties} into a {@link PropertiesTuple}.
 */
@Since("1.10.0")
@Summary("Converts a Element Properties into a PropertiesTuple")
public class ToPropertiesTuple extends KorypheFunction<Properties, PropertiesTuple> {

    @Override
    public PropertiesTuple apply(final Properties properties) {
        if (isNull(properties)) {
            return null;
        }

        return new PropertiesTuple(properties);
    }
}
