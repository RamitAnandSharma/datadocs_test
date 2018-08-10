package com.dataparse.server.service.visualization.bookmark_state.utils;

import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;

import com.dataparse.server.util.*;
import lombok.extern.slf4j.*;
import org.apache.commons.lang3.*;

import java.util.*;
import java.util.stream.*;

@Slf4j
public class QueryToString {

    private QueryParams params;
    private Map<String, Col> fieldToColumn;

    public QueryToString(final List<Col> columns,
                         final QueryParams params) {
        this.params = params;
        this.fieldToColumn = columns.stream()
                .collect(Collectors.toMap(c -> c.getField(), c -> c));
    }

    private String toString(Show s){
        return (s.getOp() == null ? "" : s.getOp() + " ") + fieldToColumn.get(s.getField()).getName();
    }

    private String toString(Agg a){
        return (a.getOp() == null ? "" : a.getOp() + " ") + fieldToColumn.get(a.getField()).getName();
    }

    private boolean isActive(FilterValue v){
        return !v.isShow() || v.isSelected();
    }

    private String toString(Object o, Col col){
        if(o == null){
            return "[empty value]";
        }
        if(col.getType().equals(DataType.STRING)){
            return "\"" + o.toString() + "\"";
        } else if (col.getType().equals(DataType.DATE)){
            Date date;
            if(o instanceof Date){
                date = (Date) o;
            } else if(o instanceof Long) {
                date = new Date((Long) o);
            } else {
                return o.toString();
            }
            return DateUtils.shortDateFormat.format(date);
        } else if (col.getType().equals(DataType.TIME)){
            Date date;
            if(o instanceof Date){
                date = (Date) o;
            } else if(o instanceof Long) {
                date = new Date((Long) o);
            } else {
                return o.toString();
            }
            return DateUtils.timeFormat.format(date);
        } else {
            return o.toString();
        }
    }

    private String toString(FilterValue v, Col col){
        if(!v.isSelected()){
            return "!" + toString(v.getKey(), col);
        } else {
            return toString(v.getKey(), col);
        }
    }

    private String toString(Number v1, Number v2, Col col){
        return toString(v1, col) + " - " + toString(v2, col);
    }

    private String toString(FixedDateType fixedDateType) {
        return StringUtils.capitalize(fixedDateType.name().replace("_", " "));
    }

    private String toString(Filter f){
        Col col = fieldToColumn.get(f.getField());
        String conj = f.isAnd_or() ? " and " : " or ";
        String filterString = fieldToColumn.get(f.getField()).getName() + " = ";

        if(f instanceof DateRangeFilter && ((DateRangeFilter) f).getFixedDate() != null){
            return filterString + toString(((DateRangeFilter) f).getFixedDate());
        } else if(f instanceof NumberRangeFilter && !f.isListMode()){
            Number v1 = ((NumberRangeFilter) f).getValue1();
            Number v2 = ((NumberRangeFilter) f).getValue2();
            return filterString + toString(v1, v2, col);
        } else {
            return filterString + f.getList().stream().filter(this::isActive).map(fv -> toString(fv, col))
                    .collect(Collectors.joining(conj));
        }
    }

    private String sortToString(Show s){
        return toString(s) + " " + s.getSettings().getSort().getDirection().name();
    }

    private String sortToString(Agg a, boolean pivot){
        AggSort sort = a.getSettings().getSort();
        if(sort.getType().equals(SortType.BY_KEY)){
            return toString(a) + " " + sort.getDirection().name();
        } else {
            List<Agg> aggs;
            if(pivot){
                aggs = params.getAggs();
            } else {
                aggs = params.getPivot();
            }
            List<String> keyStrings = new ArrayList<>();
            for(int i = 0; i < sort.getAggKeyPath().size(); i++){
                Agg agg = aggs.get(i);
                Object o = sort.getAggKeyPath().get(i);
                keyStrings.add(toString(o, fieldToColumn.get(agg.getField())));
            }
            return toString(a) + " "
                   + (sort.getIsCount() ? "Count" : fieldToColumn.get(sort.getField()))
                   + " in " + String.join(" > ", keyStrings)
                   + " " + sort.getDirection().name();
        }
    }

    private boolean isActive(Filter f){
        return f.isSelected() && f.isActive() && !f.isHidden();
    }

    public String toString(){
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Showing ");
            List<String> shows = new ArrayList<>();
            if (!params.isRaw()) {
                shows.add("Aggregation");
            }
            shows.addAll(params.getShows().stream().map(this::toString).collect(Collectors.toList()));
            sb.append(String.join(", ", shows));
            if (!params.getAggs().isEmpty()) {
                sb.append(" broken down by ");
                sb.append(params.getAggs().stream().map(this::toString).collect(Collectors.joining(", ")));
            }
            if (!params.getPivot().isEmpty()) {
                sb.append(" pivot by ");
                sb.append(params.getPivot().stream().map(this::toString).collect(Collectors.joining(", ")));
            }
            if (params.getFilters().stream().anyMatch(this::isActive) || StringUtils.isNotBlank(params.getSearch())) {
                sb.append(" filtered by ");
                List<String> clauses = params
                        .getFilters()
                        .stream()
                        .filter(this::isActive)
                        .map(this::toString)
                        .collect(Collectors.toList());
                if (StringUtils.isNotBlank(params.getSearch())) {
                    clauses.add("search = \"" + params.getSearch() + "\"");
                }
                sb.append(String.join(", ", clauses));
            }
            if (params.isRaw()) {
                if (params.getShows().stream().anyMatch(s -> s.getSettings().getSort() != null)) {
                    sb.append(" sorted by ");
                    sb.append(params.getShows().stream()
                                      .filter(s -> s.getSettings().getSort() != null)
                                      .sorted(Comparator.comparing(s2 -> s2.getSettings().getSort().getPriority()))
                                      .map(this::sortToString).collect(Collectors.joining(", ")));
                }
            } else {
                sb.append(" sorted by ");
                List<String> sortStrings = new ArrayList<>();
                sortStrings.addAll(params.getAggs().stream().map(a -> sortToString(a, false)).collect(Collectors.toList()));
                sortStrings.addAll(params.getPivot().stream().map(p -> sortToString(p, true)).collect(Collectors.toList()));
                sb.append(String.join(", ", sortStrings));
            }
            return sb.toString();
        } catch (Exception e){
            log.error("Can't convert query to string", e);
            return "<Error converting query to string>";
        }
    }

}
