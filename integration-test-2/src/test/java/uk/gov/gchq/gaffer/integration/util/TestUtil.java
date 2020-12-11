package uk.gov.gchq.gaffer.integration.util;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.util.TreeSet;

public class TestUtil {

    public static final String SOURCE = "1-Source";
    public static final String DEST = "2-Dest";
    public static final String SOURCE_DIR = "1-SourceDir";
    public static final String DEST_DIR = "2-DestDir";

    public static final String SOURCE_1 = SOURCE + 1;
    public static final String DEST_1 = DEST + 1;

    public static final String SOURCE_2 = SOURCE + 2;
    public static final String DEST_2 = DEST + 2;

    public static final String SOURCE_3 = SOURCE + 3;
    public static final String DEST_3 = DEST + 3;

    public static final String SOURCE_DIR_0 = SOURCE_DIR + 0;
    public static final String DEST_DIR_0 = DEST_DIR + 0;

    public static final String SOURCE_DIR_1 = SOURCE_DIR + 1;
    public static final String DEST_DIR_1 = DEST_DIR + 1;

    public static final String SOURCE_DIR_2 = SOURCE_DIR + 2;
    public static final String DEST_DIR_2 = DEST_DIR + 2;

    public static final String SOURCE_DIR_3 = SOURCE_DIR + 3;
    public static final String DEST_DIR_3 = DEST_DIR + 3;

    private static final long AGE_OFF_TIME = 10L * 1000; // 10 seconds;

    private TestUtil() {
        // prevent instantiation
    }

    public static Schema createDefaultSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.PROP_SET_STRING, new TypeDefinition.Builder()
                        .clazz(TreeSet.class)
                        .aggregateFunction(new CollectionConcat<>())
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new AgeOff(AGE_OFF_TIME))
                        .build())
                .type(TestTypes.PROP_INTEGER_2, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new IsLessThan(10))
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER_2)
                        .build())
                .build();
    }
}
