package uk.gov.gchq.gaffer.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.store.Context;

public class GraphResult<O> {
    private final O result;
    private final Context context;

    @JsonCreator
    public GraphResult(@JsonProperty("result") final O result, @JsonProperty("context") final Context context) {
        this.result = result;
        this.context = context;
    }

    public O getResult() {
        return result;
    }

    public Context getContext() {
        return context;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GraphResult<?> that = (GraphResult<?>) o;

        return new EqualsBuilder()
                .append(result, that.result)
                .append(context, that.context)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(result)
                .append(context)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("result", result)
                .append("context", context)
                .toString();
    }
}
