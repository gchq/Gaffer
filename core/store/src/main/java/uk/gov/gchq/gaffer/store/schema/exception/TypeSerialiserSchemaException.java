package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

import static uk.gov.gchq.gaffer.store.schema.Schema.FORMAT_EXCEPTION;
import static uk.gov.gchq.gaffer.store.schema.Schema.FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S;

public class TypeSerialiserSchemaException extends SchemaException {
    private static final String TYPE_SERIALISER = "type serialiser";
    private static final String SCHEMAS_CONFLICT_WITH_TYPE_SERIALISER = String.format(FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S, TYPE_SERIALISER);

    public TypeSerialiserSchemaException(final String serialiserName, final String serialiserClass) {
        super(String.format(FORMAT_EXCEPTION, SCHEMAS_CONFLICT_WITH_TYPE_SERIALISER, serialiserName, serialiserClass));
    }
}
