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

package uk.gov.gchq.gaffer.federated;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.koryphe.function.KorypheFunction;

public class ToElementSeed extends KorypheFunction<Element, ElementId> {
    @Override
    public ElementId apply(final Element element) {
        if (element.getGroup().endsWith("|Edge")) {
            return new EdgeSeed(
                    element.getProperty("source"),
                    element.getProperty("destination"),
                    Boolean.TRUE.equals(element.getProperty("directed"))
            );
        }

        return new EntitySeed(element.getProperty("vertex"));
    }
}
