package uk.gov.gchq.gaffer.generator;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import java.util.Arrays;

public class TestGeneratorImpl implements OneToManyElementGenerator<String> {
    @Override
    public Iterable<Element> _apply(final String domainObject) {
        return Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(domainObject)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex(domainObject)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
        );
    }
}
