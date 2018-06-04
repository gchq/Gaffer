package uk.gov.gchq.gaffer.graph.hook.migrate;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

public class TransformAndFilter extends KoryphePredicate<Element> {
    private ElementTransformer transformer;
    private ElementFilter filter;

    public TransformAndFilter() {
    }

    public TransformAndFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    public TransformAndFilter(final ElementTransformer transformer, final ElementFilter filter) {
        this.transformer = transformer;
        this.filter = filter;
    }

    @Override
    public boolean test(final Element element) {
        if (null == element) {
            return false;
        }

        if (null == filter) {
            return true;
        }

        return filter.test(null != transformer ? transformer.apply(element.shallowClone()) : element);
    }

    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    public ElementFilter getFilter() {
        return filter;
    }

    public void setFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("transformer", transformer)
                .append("filter", filter)
                .toString();
    }
}