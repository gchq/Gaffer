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
package uk.gov.gchq.gaffer.doc.properties.walkthrough;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.yahoo.sketches.frequencies.LongsSketch;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthroughRunner;
import uk.gov.gchq.gaffer.doc.walkthrough.WalkthroughStrSubstitutor;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

public class PropertiesWalkthroughRunner extends AbstractWalkthroughRunner {
    private static final List<Class<?>> SIMPLE_PROPERTIES = Arrays.asList(
            String.class,
            Long.class,
            Integer.class,
            Double.class,
            Float.class,
            Byte[].class,
            Boolean.class,
            Date.class,
            TypeValue.class,
            TypeSubTypeValue.class,
            FreqMap.class,
            HashMap.class,
            TreeSet.class
    );

    private static final List<Class<?>> SKETCHES = Arrays.asList(
            HyperLogLogPlus.class,
            HllSketch.class,
            LongsSketch.class,
            DoublesSketch.class,
            ReservoirItemsSketch.class,
            Sketch.class
    );

    private static final List<AbstractWalkthrough> CLEARSPRING_SKETCHES_WALKTHROUGHS = Arrays.asList(
            new HyperLogLogPlusWalkthrough()
    );

    private static final List<AbstractWalkthrough> DATA_SKETCHES_WALKTHROUGHS = Arrays.asList(
            new HllSketchWalkthrough(),
            new LongsSketchWalkthrough(),
            new DoublesSketchWalkthrough(),
            new ReservoirItemsSketchWalkthrough(),
            new ThetaSketchWalkthrough()
    );

    private static final List<AbstractWalkthrough> EXAMPLES = getExamples();

    private static List<AbstractWalkthrough> getExamples() {
        final List<AbstractWalkthrough> examples = new ArrayList<>();
        SIMPLE_PROPERTIES.forEach(c -> examples.add(new SimpleProperty(c)));
        SKETCHES.forEach(c -> examples.add(new SimpleProperty(c)));
        return examples;
    }

    public static void main(final String[] args) throws Exception {
        new PropertiesWalkthroughRunner().run();
    }

    public PropertiesWalkthroughRunner() {
        super(null, "doc", "properties");
    }

    @Override
    public void run() throws Exception {
        printHeader();
        printTableOfContents();
        printIntro();
        printSimpleProperties();
        printSketches();
        printExamples();
        printPredicatesAggregatorsSerialisersTitle();
        for (final AbstractWalkthrough example : EXAMPLES) {
            // Clear the caches so the output is not dependent on what's been run before
            try {
                if (CacheServiceLoader.getService() != null) {
                    CacheServiceLoader.getService().clearCache("NamedOperation");
                    CacheServiceLoader.getService().clearCache("JobTracker");
                }
            } catch (final CacheOperationException e) {
                throw new RuntimeException(e);
            }

            System.out.println(example.walkthrough());
            System.out.println(EXAMPLE_DIVIDER);
        }
    }

    private void printTableOfContents() throws InstantiationException, IllegalAccessException {
        int index = 1;
        System.out.println(index + ". [Introduction](#introduction)");
        index++;
        System.out.println(index + ". [Running the Examples](#runningtheexamples)");
        index++;
        System.out.println(index + ". [Simple properties](#simpleproperties)");
        index++;
        System.out.println(index + ". [Sketches](#sketches)");
        index++;
        System.out.println(index + ". [RoaringBitmap](#roaringbitmap)");
        index++;
        System.out.println(index + ". [Examples](#examples)");
        index++;
        // TODO - list sub examples
        System.out.println(index + ". [Predicates, aggregators and serialisers](#predicatesaggregatorsserialisers)");

        index = 1;
        for (final AbstractWalkthrough example : EXAMPLES) {
            final String header = example.getHeader();
            System.out.println("   " + index + ". [" + header + "](#" + header.toLowerCase(Locale.getDefault()).replace(" ", "-") + ")");
            index++;
        }
        System.out.println("\n");
    }

    protected void printSimpleProperties() {
        final String intro;
        try (final InputStream stream = StreamUtil.openStream(getClass(), resourcePrefix + "/walkthrough/SimpleProperties.md")) {
            intro = new String(IOUtils.toByteArray(stream), CommonConstants.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println(WalkthroughStrSubstitutor.substitute(intro, modulePath));
    }

    private void printSketches() {
        final String intro;
        try (final InputStream stream = StreamUtil.openStream(getClass(), resourcePrefix + "/walkthrough/Sketches.md")) {
            intro = new String(IOUtils.toByteArray(stream), CommonConstants.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println(WalkthroughStrSubstitutor.substitute(intro, modulePath));
    }

    private void printExamples() throws OperationException {
        printExamples(CLEARSPRING_SKETCHES_WALKTHROUGHS);
        printExamples(DATA_SKETCHES_WALKTHROUGHS);
    }

    private void printExamples(final List<AbstractWalkthrough> walkthroughs) throws OperationException {
        for (final AbstractWalkthrough example : walkthroughs) {
            // Clear the caches so the output is not dependent on what's been run before
            try {
                if (CacheServiceLoader.getService() != null) {
                    CacheServiceLoader.getService().clearCache("NamedOperation");
                    CacheServiceLoader.getService().clearCache("JobTracker");
                }
            } catch (final CacheOperationException e) {
                throw new RuntimeException(e);
            }

            System.out.println(example.walkthrough());
            System.out.println(EXAMPLE_DIVIDER);
        }
    }

    private void printPredicatesAggregatorsSerialisersTitle() {
        System.out.println("## Predicates, aggregators and serialisers");
    }
}
