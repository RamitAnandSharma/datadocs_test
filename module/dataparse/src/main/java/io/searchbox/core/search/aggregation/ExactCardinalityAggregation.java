package io.searchbox.core.search.aggregation;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static io.searchbox.core.search.aggregation.AggregationField.VALUE;

/**
 * @author cfstout
 */
public class ExactCardinalityAggregation extends MetricAggregation {

    public static final String TYPE = "exact_cardinality";

    private Long exactCardinality;

    public ExactCardinalityAggregation(String name, JsonObject exactCardinalityAggregation) {
        super(name, exactCardinalityAggregation);
        exactCardinality = exactCardinalityAggregation.get(String.valueOf(VALUE)).getAsLong();
    }

    /**
     * @return Cardinality if it was found and not null, null otherwise
     */
    public Long getExactCardinality() {
        return exactCardinality;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }

        ExactCardinalityAggregation rhs = (ExactCardinalityAggregation) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(exactCardinality, rhs.exactCardinality)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(exactCardinality)
                .toHashCode();
    }
}

