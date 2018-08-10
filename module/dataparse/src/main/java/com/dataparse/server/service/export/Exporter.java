package com.dataparse.server.service.export;

import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.parser.processor.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.visualization.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;
import lombok.extern.slf4j.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

@Slf4j
public abstract class Exporter {

    protected final static String GRAND_TOTAL_LABEL = "Grand Total";
    protected final static String TOTAL_LABEL = "Total";
    protected final static String AGGREGATION_HEADER = "Aggregation: ";
    protected final static String DOC_COUNT_FIELD = "$$cluster_size";
    protected final static String DOC_COUNT_NAME = "count";

    protected String getFileName(String datadocName, String extension) {
        return datadocName + "_export." + extension;
    }

    private void parseLowLevelKeys(Tree.Node<Map<String, Object>> headers, List<Object> path, int level,
                                   List<Show> shows, List<Agg> pivot, Map<String, Boolean> columnGroupStateCollapsed, Map<String, Show> results){
        if(headers.getParent() != null) {
            path.add(headers.getKey());
        }
        String field = TreeUtils.getField(path);
        if (path.size() == level) {
            for (Show show : shows) {
                List<Object> tmp = new ArrayList<>(path);
                tmp.add(show.key());
                results.put(TreeUtils.getField(tmp), show);
            }
        } else {
            boolean expanded = !columnGroupStateCollapsed.containsKey(field) || !columnGroupStateCollapsed.get(field);
            if (expanded) {
                for (Tree.Node<Map<String, Object>> children : headers.getChildren()) {
                    parseLowLevelKeys(children, new ArrayList<>(path), level, shows, pivot, columnGroupStateCollapsed,
                                      results);
                }
            }
            if (!expanded || pivot.get(path.size()).getSettings().getShowTotal()) {
                for (Show show : shows) {
                    List<Object> tmp = new ArrayList<>(path);
                    tmp.add(show.key());
                    results.put(TreeUtils.getField(tmp), show);
                }
            }
        }
    }

    protected Map<String, Show> getHeaderKeys(Tree.Node<Map<String, Object>> headers, List<Agg> pivot, List<Show> shows, Map<String, Boolean> columnGroupStateCollapsed){
        Map<String, Show> results = new LinkedHashMap<>();
        if(pivot.isEmpty()){
            shows.forEach(show -> results.put(show.key(), show));
        } else {
            List<Show> showsCopy = shows;
            if(shows.isEmpty()){
                showsCopy = new ArrayList<>();
                showsCopy.add(new Show(DOC_COUNT_FIELD));
            }
            parseLowLevelKeys(headers, new ArrayList<>(), pivot.size(), showsCopy, pivot, columnGroupStateCollapsed, results);
        }
        return results;
    }

    private int getDepth(Tree.Node<?> node){
        int depth = 0;
        while(node.getParent() != null){
            node = node.getParent();
        }
        return depth;
    }

    private void createPathList(Tree.Node<Map<String, Object>> node, List<List<String>> paths,
                                Map<String, Col> columnsByFieldName, BookmarkState state, List<Processor> processors){
        List<Show> shows = state.getQueryParams().getShows();
        List<Show> showsCopy = shows;
        if(shows.isEmpty()){
            showsCopy = new ArrayList<>();
            showsCopy.add(new Show(DOC_COUNT_FIELD));
        }

        List<Agg> pivot = state.getQueryParams().getPivot();

        List<Object> path = TreeUtils.getParentKeysPath(node);
        path = path.subList(1, path.size());
        String pivotKey = TreeUtils.getField(path);

        List<String> namesPath = new ArrayList<>(path.size());

        // format headers
        for (int i = 0; i < path.size(); i++) {
            Agg pivotItem = pivot.get(i);
            Col col = columnsByFieldName.get(pivotItem.getField());
            Object value = path.get(i);
            for (Processor processor : processors) {
                value = processor.format(value, col, pivotItem.getOp());
            }
            namesPath.add(TreeUtils.getField(Collections.singletonList(value)));
        }

        if (path.size() == state.getQueryParams().getPivot().size()) {
            // format shows
            if (showsCopy.size() > 1) {
                for (Show show : showsCopy) {
                    Col col = columnsByFieldName.get(show.getField());
                    String showName = getShowName(show, col);
                    List<String> pathCopy = new ArrayList<>(namesPath);
                    pathCopy.add(showName);
                    paths.add(pathCopy);
                }
            } else {
                paths.add(namesPath);
            }
        } else {
            boolean expanded = !state.getPivotCollapsedState().containsKey(pivotKey) || !state.getPivotCollapsedState().get(pivotKey);

            for (Tree.Node<Map<String, Object>> header : node.getChildren()) {
                if (expanded) {
                    createPathList(header, paths, columnsByFieldName, state, processors);
                }
            }
            if (!expanded || pivot.get(path.size()).getSettings().getShowTotal()) {
                for (Show show : showsCopy) {
                    Col col = columnsByFieldName.get(show.getField());
                    String showName = getShowName(show, col);
                    List<String> pathCopy = new ArrayList<>(namesPath);
                    String label = path.size() == 0 ? GRAND_TOTAL_LABEL : TOTAL_LABEL;
                    for(int i = 0; i < state.getQueryParams().getPivot().size() - path.size() - 1; i++){
                        pathCopy.add(i == 0 ? label : "");
                    }
                    if(pivot.size() == path.size() + 1 && showsCopy.size() == 1) {
                        pathCopy.add(label);
                    } else {
                        pathCopy.add(showName);
                    }
                    paths.add(pathCopy);
                }
            }
        }
    }

    protected List<List<String>> getHeadersAsList(Tree.Node<Map<String, Object>> headers, BookmarkState state,
                                                  List<Processor> processors){
        List<Col> cols = state.getColumnList();
        Map<String, Col> columnsByFieldName = cols.stream().collect(Collectors.toMap(Col::getField, c -> c));
        List<List<String>> groupHeaders = new ArrayList<>();
        List<List<String>> pathList = new ArrayList<>();

        createPathList(headers, pathList, columnsByFieldName, state, processors);

        List<String> lastColumnParents = null;
        for (List<String> parents : pathList) {
            boolean newHeader = false;
            for(int i = 0; i < parents.size(); i++){
                String parent = parents.get(i);
                if(newHeader || lastColumnParents == null || !Objects.equals(parent, lastColumnParents.get(i))) {
                    newHeader = true;
                }
                if(newHeader){
                    List<String> groupHeaderList;
                    try{
                        groupHeaderList = groupHeaders.get(i);
                    } catch (Exception e){
                        groupHeaderList = new ArrayList<>();
                        groupHeaders.add(i, groupHeaderList);
                    }
                    groupHeaderList.add(parent);

                } else {
                    groupHeaders.get(i).add(""); // add empty space for duplicate header
                }
            }
            lastColumnParents = parents;
        }
        return groupHeaders;
    }

    private String getShowName(Show show, Col col){
        if(show.key().equals(DOC_COUNT_FIELD)){
            return DOC_COUNT_NAME;
        }
        return (show.getOp() == null ? "" : show.getOp().name() + " ") + col.getName();
    }

    protected List<List<String>> getHeaderNames(Tree.Node<Map<String, Object>> headers, BookmarkState state, List<Processor> processors) {

        QueryParams params = state.getQueryParams();
        List<Col> cols = state.getColumnList();

        Map<String, Col> columnsByFieldName = cols.stream().collect(Collectors.toMap(Col::getField, c -> c));
        List<List<String>> headerNamesList;
        if(params.getPivot().isEmpty()){
            headerNamesList = new ArrayList<>();
            headerNamesList.add(params.getShows()
                                        .stream()
                                        .map(show -> {
                                            Col col = columnsByFieldName.get(show.getField());
                                            return getShowName(show, col);
                                        })
                                        .collect(Collectors.toList()));
        } else {
            Tree.Node<Map<String, Object>> mappedHeaders = TreeUtils.mapTreeKey(headers, (n) -> {
                int depth = getDepth(n);
                if (depth <= 0) {
                    return n.getKey();
                }
                Agg agg = params.getPivot().get(depth - 1);
                String field = TreeUtils.getField(Collections.singletonList(n.getKey()));
                Col col;
                if (field.equals(DOC_COUNT_FIELD)) {
                    col = new Col(field, field, field, DataType.DECIMAL);
                } else {
                    col = columnsByFieldName.get(agg.getField());
                }
                Object key = n.getKey();
                for (Processor processor : processors) {
                    key = processor.format(key, col, agg.getOp());
                }
                return key;
            });
            headerNamesList = getHeadersAsList(mappedHeaders, state, processors);
        }

        if(params.getAggs().size() > 0) {
            String aggregationPath = params.getAggs().stream()
                    .map(agg -> {
                        Col col = columnsByFieldName.get(agg.getField());
                        return col.getName() + (agg.getOp() == null ? "" : "(" + agg.getOp() + ")" );
                    })
                    .collect(Collectors.joining(" > "));
            int i = 0;
            for(List<String> headerNames: headerNamesList) {
                headerNames.add(0, ++i < headerNamesList.size() ? "" : AGGREGATION_HEADER + aggregationPath);
            }
        }
        return headerNamesList;
    }

    @SuppressWarnings("unchecked")
    protected List getRow(List<String> showKeys, List<String> aggKeys, Map<String, Object> o){
        int size = showKeys.size();
        if(!aggKeys.isEmpty()){
            size++;
        }
        List row = new ArrayList(size);
        if(!aggKeys.isEmpty()){
            row.add(aggKeys.stream()
                    .filter(o::containsKey)
                    .map(key -> {
                        Object v = o.get(key);
                        if(v == null){
                            return "";
                        }
                        return v.toString();
                    })
                    .collect(Collectors.joining(" > ")));
        }
        for(String showKey : showKeys){
            Object v = o.get(showKey);
            if(showKey.endsWith(DOC_COUNT_FIELD) && v == null){
                v = 0;
            }
            row.add(v);
        }
        return row;
    }

    public abstract Descriptor export(List<ExportItemParams> exportItems,
                                          Consumer<Double> progressCallback, String datadocName);

    @Data
    @AllArgsConstructor
    public static class ExportItemParams {
        private String name;
        private BookmarkState state;
        private SearchIndexRequest request;
        private int limit;
        private List<Processor> processors;
    }

}
