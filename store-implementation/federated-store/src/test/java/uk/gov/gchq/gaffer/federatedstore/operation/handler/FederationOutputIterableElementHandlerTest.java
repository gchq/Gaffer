package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.io.Output;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class FederationOutputIterableElementHandlerTest<OP extends Output<CloseableIterable<? extends Element>>> extends FederationOutputIterableHandlerTest<OP, Element> {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        o1 = new WrappedCloseableIterable<>(Lists.<Element>newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 1)
                        .build()));
        o2 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 2)
                        .build()));
        o3 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 3)
                        .build()));
        o4 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 2)
                        .build()));
    }

}
