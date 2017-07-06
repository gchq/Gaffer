package uk.gov.gchq.gaffer.generator;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class TestGeneratorImpl implements OneToOneElementGenerator<String> {
    @Override
    public Element _apply(final String domainObject) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(domainObject)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
    }
}