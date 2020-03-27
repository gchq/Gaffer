package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleToSplitPoints;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.spark.operation.javardd.SplitStoreFromJavaRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.List;

public abstract class AbstractSplitStoreFromRDDOfElementsHandler<OP extends Operation> implements OperationHandler<OP> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SplitStoreFromJavaRDDOfElements.class);
    private static final double DEFAULT_FRACTION_TO_SAMPLE = 0.001d;
    private static final int DEFAULT_MAX_SAMPLE_SIZE = 10_000_000;

    protected double adjustFractionToSampleForSize(final Double fractionToSample, final Integer maxSampleSize, final long rddCount) {

        double configuredFractionToSample = fractionToSample != null ? fractionToSample : DEFAULT_FRACTION_TO_SAMPLE;
        int configuredMaxSampleSize = maxSampleSize != null ? maxSampleSize : DEFAULT_MAX_SAMPLE_SIZE;
        double adjustedFractionToSample = configuredFractionToSample;

        final double expectedSampledRowCount = rddCount * configuredFractionToSample;

        if (expectedSampledRowCount > configuredMaxSampleSize) {
            adjustedFractionToSample = rddCount / (double) configuredMaxSampleSize;
            LOGGER.warn("The configured fractionToSample: {} would exceed the configured maxSampleSize of {} when sampling an RDD containg {} rows. Compensating by reducing fractionToSample to {}.",
                    configuredFractionToSample,
                    rddCount,
                    configuredMaxSampleSize,
                    adjustedFractionToSample);
        }

        return adjustedFractionToSample;
    }

    protected void createSplitPoints(final AccumuloStore store, final Context context, final List<String> sample) throws OperationException {

        store.execute(
                new OperationChain.Builder()
                        .first(new SampleToSplitPoints.Builder<>()
                                .input(sample)
                                .build())
                        .then(new SplitStoreFromIterable.Builder<>()
                                .build())
                        .build(),
                context);
    }
}
