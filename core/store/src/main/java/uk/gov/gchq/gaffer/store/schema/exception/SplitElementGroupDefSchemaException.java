package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

public class SplitElementGroupDefSchemaException extends SchemaException {

    private static final String ELEMENT_GROUP_MUST_ALL_BE_DEFINED_IN_A_SINGLE_SCHEMA = "Element group properties cannot be defined in different schema parts, they must all be defined in a single schema part. Please fix this group: ";

    public SplitElementGroupDefSchemaException(final String sharedGroup) {
        super(ELEMENT_GROUP_MUST_ALL_BE_DEFINED_IN_A_SINGLE_SCHEMA + sharedGroup);
    }
}
