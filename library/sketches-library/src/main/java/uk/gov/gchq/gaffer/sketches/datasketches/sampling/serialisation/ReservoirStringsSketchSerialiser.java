/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation;

import org.apache.datasketches.common.ArrayOfStringsSerDe;

/**
 * A {@code ReservoirStringsSketchSerialiser} serialises a {@link org.apache.datasketches.sampling.ReservoirItemsSketch}
 * of {@link String}s using its {@code toByteArray()} method.
 */
public class ReservoirStringsSketchSerialiser extends ReservoirItemsSketchSerialiser<String> {
    private static final long serialVersionUID = 5852905480385068258L;

    public ReservoirStringsSketchSerialiser() {
        super(new ArrayOfStringsSerDe());
    }
}

