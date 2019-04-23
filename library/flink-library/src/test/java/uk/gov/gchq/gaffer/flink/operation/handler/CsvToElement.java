/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.operation.handler;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class CsvToElement implements OneToOneElementGenerator<String> {
    @Override
    public Element _apply(final String csv) {
        if (null == csv) {
            System.err.println("CSV is required in format [source],[destination]");
            return null;
        }
        final String[] parts = csv.split(",");
        if (2 != parts.length) {
            System.err.println("CSV is required in format [source],[destination]");
            return null;
        }

        return new Edge.Builder()
                .group("edge")
                .source(parts[0].trim())
                .dest(parts[1].trim())
                .directed(true)
                .property("count", 1)
                .build();
    }
}
