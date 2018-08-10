package com.dataparse.server.service.bigquery.cache;

import com.dataparse.server.service.bigquery.cache.serialization.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.request.*;
import com.google.common.base.Charsets;
import com.google.common.hash.*;
import org.apache.commons.lang3.*;

import java.util.*;
import java.util.stream.*;

public class QueryKey {

    private static class Key {

        public static class FilterWrapper {
            private Filter filter;
            private List<FilterValue> values;

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                FilterWrapper that = (FilterWrapper) o;
                if(!filter.getField().equals(that.filter.getField())){
                    return false;
                }
                if(!filter.getClass().equals(that.filter.getClass())){
                    return false;
                }
                if(filter.isAnd_or() != that.filter.isAnd_or()){
                    return false;
                }
                if(filter.isListMode() != that.filter.isListMode()){
                    return false;
                }
                if(filter.isListMode()) {
                    if (!values.equals(that.values)) {
                        return false;
                    }
                } else {
                    NumberRangeFilter f1 = (NumberRangeFilter) this.filter;
                    NumberRangeFilter f2 = (NumberRangeFilter) that.filter;
                    if (!Objects.equals(f1.getValue1(), f2.getValue1())) {
                        return false;
                    }
                    if (!Objects.equals(f1.getValue2(), f2.getValue2())) {
                        return false;
                    }
                    if (filter instanceof DateRangeFilter) {
                        DateRangeFilter df1 = (DateRangeFilter) this.filter;
                        DateRangeFilter df2 = (DateRangeFilter) that.filter;
                        if (!Objects.equals(df1.getFixedDate(), df2.getFixedDate())) {
                            return false;
                        }
                    }
                }
                return true;
            }

            public static FilterWrapper wrap(Filter f){
                FilterWrapper wrapper = new FilterWrapper();
                wrapper.filter = f;
                wrapper.values = f.getList().stream()
                        .filter(v -> v.isSelected() || !v.isShow())
                        .collect(Collectors.toList());
                wrapper.values.sort(
                        (o1, o2) -> ObjectUtils.compare((Comparable) o1.getKey(), (Comparable) o2.getKey()));
                return wrapper;
            }
        }

        private List<Col> cols;
        private List<Show> shows;
        private List<Agg> rowAggs;
        private List<Agg> colAggs;
        private List<FilterWrapper> filters;

        private String search;

        private boolean count;
        private Integer pageNumber;
        private Long pageSize;


        public static Key of(BigQueryRequest request){
            if(request.getRequest().isFacetQuery()){
                return null;
            }
            Key key = new Key();
            key.count = request.getRequest() instanceof CountRequest;
            key.pageNumber = request.getPageNumber();
            key.pageSize = request.getPageSize();
            key.cols = request.getRequest().getColumns();
            key.shows = request.getRequest().getParams().getShows();
            key.rowAggs = request.getRequest().getParams().getAggs();
            key.colAggs = request.getRequest().getParams().getPivot();
            key.filters = request.getRequest().getParams().getFilters().stream().map(FilterWrapper::wrap).collect(
                    Collectors.toList());
            key.search = request.getRequest().getParams().getSearch();
            return key;
        }
    }

    private String sKey;
    private String hash;
    private Key key;

    private QueryKey(final String hash, final String accountId, final String externalId, final Key key) {
        this.hash = hash;
        this.sKey = accountId + ":" + externalId;
        this.key = key;
    }

    public static QueryKey of(final BigQueryRequest request){
        return new QueryKey(getQueryHashKey(request),
                            request.getRequest().getAccountId(),
                            request.getRequest().getExternalId(),
                            Key.of(request));
    }

    private static String getQueryHashKey(BigQueryRequest request){
        HashFunction hf = Hashing.murmur3_32();
        Hasher hasher = hf.newHasher()
                .putString(request.getQuery(), Charsets.UTF_8);
        if(request.getPageNumber() != null){
            hasher.putInt(request.getPageNumber());
        }
        if(request.getPageSize() != null){
            hasher.putLong(request.getPageSize());
        }
        HashCode hc = hasher.hash();
        return hc.toString();
    }

    public boolean isSubSetOf(final QueryKey that) {
        if(that == null){
            return false;
        }
        if(!key.count == that.key.count){
            return false;
        }
        if(!key.count) {
            if(!Objects.equals(key.pageNumber, that.key.pageNumber)){
                return false;
            }
            if(!Objects.equals(key.pageSize, that.key.pageSize)){
                return false;
            }
            if (!key.rowAggs.equals(that.key.rowAggs)) {
                return false;
            }
            if (!aggSortEquals(key.rowAggs, that.key.rowAggs)) {
                return false;
            }
            if (!key.colAggs.equals(that.key.colAggs)) {
                return false;
            }
            if (!aggSortEquals(key.colAggs, that.key.colAggs)) {
                return false;
            }
        }
        if(!key.search.equals(that.key.search)){
            return false;
        }
        if(!that.key.shows.containsAll(key.shows)){
            return false;
        }
        if(!key.count && !showSortEquals(that.key.shows, key.shows)){
            return false;
        }
        if(!searchTypeEquals(that.key.cols, key.cols)){
            return false;
        }
        if(!that.key.filters.containsAll(key.filters)){
            return false;
        }
        return true;
    }

    private boolean searchTypeEquals(List<Col> o1, List<Col> o2){
        Map<String, Col> o1Keys = o1.stream().collect(Collectors.toMap(s -> s.getField(), s -> s));
        Map<String, Col> o2Keys = o2.stream().collect(Collectors.toMap(s -> s.getField(), s -> s));
        for(Map.Entry<String, Col> entry: o1Keys.entrySet()){
            Col s1 = entry.getValue();
            Col s2 = o2Keys.get(entry.getKey());
            if(s2 == null){
                continue;
            }
            if(!Objects.equals(s1.getSettings().getSearchType(), s2.getSettings().getSearchType())){
                return false;
            }
        }
        return true;
    }

    private boolean showSortEquals(List<Show> o1, List<Show> o2){
        Map<String, Show> o1Keys = o1.stream().collect(Collectors.toMap(s -> s.key(), s -> s));
        Map<String, Show> o2Keys = o2.stream().collect(Collectors.toMap(s -> s.key(), s -> s));
        for(Map.Entry<String, Show> entry: o1Keys.entrySet()){
            Show s1 = entry.getValue();
            Show s2 = o2Keys.get(entry.getKey());
            if(s2 == null){
                continue;
            }
            if(!Objects.equals(s1.getSettings().getSort(), s2.getSettings().getSort())){
                return false;
            }
        }
        return true;
    }

    private boolean aggSortEquals(List<Agg> o1, List<Agg> o2){
        Map<String, Agg> o1Keys = o1.stream().collect(Collectors.toMap(s -> s.key(), s -> s));
        Map<String, Agg> o2Keys = o2.stream().collect(Collectors.toMap(s -> s.key(), s -> s));
        for(Map.Entry<String, Agg> entry: o1Keys.entrySet()){
            Agg a1 = entry.getValue();
            Agg a2 = o2Keys.get(entry.getKey());
            if(a2 == null){
                return false;
            }
            if(!Objects.equals(a1.getSettings().getSort(), a2.getSettings().getSort())){
                return false;
            }
        }
        return true;
    }

    public String getExternalId() {
        return sKey;
    }

    public boolean isCacheable(){
        return key != null;
    }

    public String getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return hash;
    }
}
