package com.dataparse.server.service.visualization;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.core.search.aggregation.Aggregation;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopHitsAggregation extends Aggregation {

    public static final String TYPE = "hits";

    private List<Map<String, Object>> hits = new ArrayList<>();

    public TopHitsAggregation(String name, JsonObject termAggregation) {
        super(name, termAggregation);

        if (termAggregation.has(TYPE) && termAggregation.get(TYPE).isJsonObject() && termAggregation.get(TYPE).getAsJsonObject().get(TYPE).isJsonArray()) {
            parseBuckets(termAggregation.get(TYPE).getAsJsonObject().get(TYPE).getAsJsonArray());
        }
    }

    private void parseBuckets(JsonArray hitsSources) {
        List<Map<String, Object>> tuples = new ArrayList<>();
        hitsSources.forEach(hitSource -> {
            hitSource.getAsJsonObject().get("_source").getAsJsonObject().get("tuples").getAsJsonArray().forEach(t -> {
                Map<String, Object> tuple = new HashMap<>();
                t.getAsJsonObject().entrySet().forEach(v -> tuple.put(v.getKey(), v.getValue().getAsString()));
                tuples.add(tuple);
            });
        });
        hits.add(ImmutableMap.of("tuples", tuples));
    }

    public List<Map<String, Object>> getHits() {
        return hits;
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

        TopHitsAggregation rhs = (TopHitsAggregation) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(hits, rhs.hits)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(hits)
                .toHashCode();
    }
}
