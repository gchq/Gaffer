package uk.gov.gchq.gaffer.sketches.datasketches.cardinality;

import com.yahoo.sketches.hll.HllSketch;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HllSketchEntityGeneratorTest {
    private static final String DEFAULT_ENTITY_GROUP = "Cardinality";
    private static final String DEFAULT_PROPERTY_NAME = "cardinality";
    private static final String A = "A";
    private static final String B = "B";
    private static final String PROP1 = "prop1";
    private static final String VALUE1 = "value1";

    @Test
    public void shouldCreateSimpleEntities(){
        //Given
        HllSketchEntityGenerator hllSketchEntityGenerator = new HllSketchEntityGenerator();
        Edge edge = new Edge.Builder()
                .group(TestGroups.ENTITY)
                .source(A)
                .dest(B)
                .build();
        List<? extends Element> edges = Arrays.asList(edge);

        //When
        Iterable<? extends Element> elements = hllSketchEntityGenerator.apply(edges);

        //Then
        Iterator<? extends Element> elementIterator = elements.iterator();
        Edge edgeResult = (Edge) elementIterator.next();
        Entity entityResultA = (Entity) elementIterator.next();
        Entity entityResultB = (Entity) elementIterator.next();

        assertThat(elementIterator.hasNext()).isFalse();
        assertThat(edgeResult).isEqualTo(edge);

        assertThat(entityResultA.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultA.getVertex()).isEqualTo(A);
        HllSketch entityCardinalityA = (HllSketch) entityResultA.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityA.getEstimate()).isEqualTo(1);

        assertThat(entityResultB.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultB.getVertex()).isEqualTo(B);
        HllSketch entityCardinalityB = (HllSketch) entityResultB.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityB.getEstimate()).isEqualTo(1);
    }

    @Test
    public void shouldCreateSimpleEntitiesWithProperties(){
        //Given
        HllSketchEntityGenerator hllSketchEntityGenerator = new HllSketchEntityGenerator();
        hllSketchEntityGenerator.propertyToCopy(PROP1);
        Edge edge = new Edge.Builder()
                .group(TestGroups.ENTITY)
                .source(A)
                .dest(B)
                .property(PROP1, VALUE1)
                .build();
        List<? extends Element> edges = Arrays.asList(edge);

        //When
        Iterable<? extends Element> elements = hllSketchEntityGenerator.apply(edges);

        //Then
        Iterator<? extends Element> elementIterator = elements.iterator();
        Edge edgeResult = (Edge) elementIterator.next();
        Entity entityResultA = (Entity) elementIterator.next();
        Entity entityResultB = (Entity) elementIterator.next();

        assertThat(elementIterator.hasNext()).isFalse();
        assertThat(edgeResult).isEqualTo(edge);

        assertThat(entityResultA.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultA.getVertex()).isEqualTo(A);
        HllSketch entityCardinalityA = (HllSketch) entityResultA.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityA.getEstimate()).isEqualTo(1);
        assertThat(entityResultA.getProperty(PROP1)).isEqualTo(VALUE1);

        assertThat(entityResultB.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultB.getVertex()).isEqualTo(B);
        HllSketch entityCardinalityB = (HllSketch) entityResultB.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityB.getEstimate()).isEqualTo(1);
        assertThat(entityResultB.getProperty(PROP1)).isEqualTo(VALUE1);
    }

}
