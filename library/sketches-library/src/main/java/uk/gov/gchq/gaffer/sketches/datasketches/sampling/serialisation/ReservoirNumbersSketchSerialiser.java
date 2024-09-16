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

import org.apache.datasketches.common.ArrayOfNumbersSerDe;

/**
 * A {@code ReservoirNumbersSketchSerialiser} serialises a {@link org.apache.datasketches.sampling.ReservoirItemsSketch}
 * using its {@code toByteArray()} method.
 */
public class ReservoirNumbersSketchSerialiser extends ReservoirItemsSketchSerialiser<Number> {
    private static final long serialVersionUID = 5004973617990086623L;

    public ReservoirNumbersSketchSerialiser() {
        super(new ArrayOfNumbersSerDe());
    }
}

