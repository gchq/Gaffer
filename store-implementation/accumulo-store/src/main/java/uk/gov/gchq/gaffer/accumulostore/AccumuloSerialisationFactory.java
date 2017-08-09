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

package uk.gov.gchq.gaffer.accumulostore;

import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.HyperLogLogPlusSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation.LongsSketchSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation.StringsSketchSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation.DoublesUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation.StringsUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirLongsUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirNumbersUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirStringsUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.theta.serialisation.UnionSerialiser;
import uk.gov.gchq.gaffer.store.SerialisationFactory;

/**
 * A <code>AccumuloSerialisationFactory</code> holds a list of Accumulo serialisers and
 * is design to provide compatible serialisers for given object classes.
 */
public class AccumuloSerialisationFactory extends SerialisationFactory {
    private static final Serialiser[] ACCUMULO_SERIALISERS = new Serialiser[]{
            new HyperLogLogPlusSerialiser(),
            new LongsSketchSerialiser(),
            new StringsSketchSerialiser(),
            new DoublesUnionSerialiser(),
            new StringsUnionSerialiser(),
            new ReservoirLongsUnionSerialiser(),
            new ReservoirNumbersUnionSerialiser(),
            new ReservoirStringsUnionSerialiser(),
            new UnionSerialiser()
    };

    /**
     * Constructor.
     * Adds default serialisers from super constructor and custom Accumulo serialisers
     */
    public AccumuloSerialisationFactory() {
        super();
        addSerialisers(ACCUMULO_SERIALISERS);
    }
}
