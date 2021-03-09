package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOutputCloseableIterableHandler;
import uk.gov.gchq.gaffer.operation.io.Output;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class FederationOutputIterableHandlerTest<OP extends Output<CloseableIterable<? extends ITERABLE_ELEMENT>>, ITERABLE_ELEMENT> extends FederatedOutputOperationHandlerTest<OP, CloseableIterable<? extends ITERABLE_ELEMENT>> {

    @Override
    protected boolean validateMergeResultsFromFieldObjects(final CloseableIterable<? extends ITERABLE_ELEMENT> result, final CloseableIterable<? extends ITERABLE_ELEMENT>... resultParts) {
        assertNotNull(result);
        final Iterable[] resultPartItrs = Arrays.copyOf(resultParts, resultParts.length, Iterable[].class);
        final ArrayList<Object> elements = Lists.newArrayList(new ChainedIterable<>(resultPartItrs));
        int i = 0;
        for (ITERABLE_ELEMENT e : result) {
            assertTrue(e instanceof Entity);
            elements.contains(e);
            i++;
        }
        assertEquals(elements.size(), i);
        return true;
    }

//    protected abstract FederatedOutputCloseableIterableHandler<OP, ITERABLE_ELEMENT> getFederationOperationHandler();

}
