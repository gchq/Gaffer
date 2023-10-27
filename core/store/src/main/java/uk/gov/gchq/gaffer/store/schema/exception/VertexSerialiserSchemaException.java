package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class VertexSerialiserSchemaException extends SchemaException {
    public static final String VERTEX_SERIALISER = "vertex serialiser";
    private static final String SCHEMAS_CONFLICT_WITH_VERTEX_SERIALISER = String.format(Schema.FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S, VERTEX_SERIALISER);

    public VertexSerialiserSchemaException(final String message, final Throwable e) {
        super(message, e);
    }

    public VertexSerialiserSchemaException(final String thisVertexSerialiser, final String thatVertexSerialiser) {
        super(String.format(Schema.FORMAT_EXCEPTION, SCHEMAS_CONFLICT_WITH_VERTEX_SERIALISER, thisVertexSerialiser, thatVertexSerialiser));
    }
}
