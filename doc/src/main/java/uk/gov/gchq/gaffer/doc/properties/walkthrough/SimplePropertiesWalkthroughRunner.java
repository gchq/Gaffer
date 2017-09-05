package uk.gov.gchq.gaffer.doc.properties.walkthrough;

import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthroughRunner;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.*;

/**
 * Predicates, aggregators and serialisers for the simple properties listed in <code>SIMPLE_EXAMPLES</code>.
 */
public class SimplePropertiesWalkthroughRunner extends AbstractWalkthroughRunner {
    private static final List<Class<?>> SIMPLE_EXAMPLES = Arrays.asList(
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

    private static final List<AbstractWalkthrough> EXAMPLES = getExamples();

    private static List<AbstractWalkthrough> getExamples() {
        final List<AbstractWalkthrough> examples = new ArrayList<>();
        SIMPLE_EXAMPLES.forEach(c -> examples.add(new SimpleProperty(c)));
        return examples;
    }

    public static void main(final String[] args) throws Exception {
        new SimplePropertiesWalkthroughRunner().run();
    }

    public SimplePropertiesWalkthroughRunner() {
        super(EXAMPLES, "doc", "properties");
    }
}
