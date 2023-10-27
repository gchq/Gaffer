package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class VisibilityPropertySchemaException extends SchemaException {
    public static final String VISIBILITY_PROPERTY = "visibility property";
    private static final String SCHEMAS_CONFLICT_WITH_VISIBILITY_PROPERTY = String.format(Schema.FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S, VISIBILITY_PROPERTY);

    public VisibilityPropertySchemaException(final String message, final Throwable e) {
        super(message, e);
    }

    public VisibilityPropertySchemaException(final String thisVisibilityProperty, final String thatVisibilityProperty) {
        super(String.format(Schema.FORMAT_EXCEPTION, SCHEMAS_CONFLICT_WITH_VISIBILITY_PROPERTY, thisVisibilityProperty, thatVisibilityProperty));
    }
}
