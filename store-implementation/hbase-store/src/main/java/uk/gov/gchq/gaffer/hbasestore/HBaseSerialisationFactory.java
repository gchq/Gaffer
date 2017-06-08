package uk.gov.gchq.gaffer.hbasestore;

import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.SerialisationFactory;
import uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation.LongsSketchSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation.StringsSketchSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation.DoublesUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation.StringsUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirLongsUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirNumbersUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirStringsUnionSerialiser;
import uk.gov.gchq.gaffer.sketches.datasketches.theta.serialisation.UnionSerialiser;
import uk.gov.gchq.gaffer.sketches.serialisation.HyperLogLogPlusSerialiser;

/**
 * Created on 08/06/2017.
 */
public class HBaseSerialisationFactory extends SerialisationFactory {
    private static final Serialiser[] HBASE_SERIALISERS = new Serialiser[]{
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
     * Adds default serialisers from super constructor and custom HBase serialisers
     */
    public HBaseSerialisationFactory() {
        super();
        addSerialisers(HBASE_SERIALISERS);
    }
}
