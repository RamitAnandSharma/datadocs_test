package io.searchbox.core.search.aggregation;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Aggregation {
    protected String name;
    protected JsonObject jsonRoot;
    protected JsonObject meta;
    protected List<String> fields;

    public Aggregation(String name, JsonObject jsonRoot) {
        this.name = name;
        this.jsonRoot = jsonRoot;
    }

    public String getName() {
        return name;
    }

    public String getMeta(String key) {
        if (meta == null) {
            meta = jsonRoot.getAsJsonObject("meta");
        }
        return meta == null ? null : meta.getAsJsonObject().get(key).getAsString();
    }

    public List<String> getFields() {
        if (fields == null) {
            fields = jsonRoot.entrySet().stream().filter(e -> e.getValue().isJsonObject()).map(Map.Entry::getKey).collect(Collectors.toList());
        }
        return fields;
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
        Aggregation rhs = (Aggregation) obj;
        return new EqualsBuilder()
                .append(name, rhs.name)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(name)
                .toHashCode();
    }

}
