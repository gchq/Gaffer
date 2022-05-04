package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonCreator;

import uk.gov.gchq.koryphe.impl.function.IterableFlatten;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.nonNull;

@Deprecated
public class FederatedIterableFlatten<I_ITEM> extends IterableFlatten<I_ITEM> {

    @JsonCreator()
    public FederatedIterableFlatten(@JsonProperty("operator") final BinaryOperator<I_ITEM> operator) {
        super(operator);
    }

    @Override
    public I_ITEM apply(final Iterable<I_ITEM> items) {
        if (nonNull(items)) {
            final Collection<I_ITEM> nonNulls = StreamSupport.stream(items.spliterator(), false)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return super.apply(nonNulls);
        }

        return null;
    }
}
