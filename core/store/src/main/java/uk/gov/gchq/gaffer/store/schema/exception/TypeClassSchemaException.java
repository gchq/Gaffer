package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

import static uk.gov.gchq.gaffer.store.schema.Schema.FORMAT_EXCEPTION;
import static uk.gov.gchq.gaffer.store.schema.Schema.FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S;

public class TypeClassSchemaException extends SchemaException {
    private static final String TYPE_CLASS = "type class";
    private static final String SCHEMAS_CONFLICT_WITH_TYPE_CLASS = String.format(FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S, TYPE_CLASS);

    public TypeClassSchemaException(final String className, final String typeClassName) {
        super(String.format(FORMAT_EXCEPTION, SCHEMAS_CONFLICT_WITH_TYPE_CLASS, className, typeClassName));
    }
}
