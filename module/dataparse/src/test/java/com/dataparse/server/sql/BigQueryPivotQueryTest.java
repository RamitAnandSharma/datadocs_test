package com.dataparse.server.sql;

import com.dataparse.server.*;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.request.*;
import com.dataparse.server.service.visualization.request_builder.*;
import com.google.common.collect.*;
import lombok.extern.slf4j.*;
import org.junit.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;

@Slf4j
public class BigQueryPivotQueryTest extends IsolatedContextTest {

    @Autowired
    private BigQueryRequestExecutor executor;

    @Test
    @Ignore
    public void simpleQueryTest() {
        List<Show> select = ImmutableList.of(new Show("royalty_price", Op.SUM));

        List<Agg> aggs = ImmutableList.of(new Agg("country_code"), new Agg("provider"));
        aggs.get(0).getSettings().setSort(new AggSort(SortDirection.DESC, true));
        aggs.get(1).getSettings().setSort(new AggSort(SortDirection.DESC, true));

        List<Filter> filters = ImmutableList.of(new DoubleRangeFilter("royalty_price", 0., 1068., 0L));
        ((DoubleRangeFilter) filters.get(0)).setValue1(10.);
        ((DoubleRangeFilter) filters.get(0)).setValue2(1000.);

        QueryParams params = new QueryParams();
        params.setShows(select);
        params.setAggs(aggs);
        params.setFilters(filters);
        params.getLimit().setPageSize(100);

        String table = "csv_example.sales_testing_data_100k";

        List<Col> columns = new ArrayList<>();
        columns.add(new Col("provider", "c1", DataType.STRING));
        columns.add(new Col("title", "c2", DataType.STRING));
        columns.add(new Col("royalty_price", "c3", DataType.DECIMAL));
        columns.add(new Col("date", "c4", DataType.STRING));
        columns.add(new Col("country_code", "c5", DataType.STRING));
        SearchIndexResponse response = executor.search(new SearchRequest(1L, 1L, 1L, "woven-nimbus-130608", table, columns, params, new HashMap<>()));
        log.info("Count: \n\t{}",- response.getCount());
        log.info("Data: \n\t{}", response.getData());

        response = executor.search(new SearchRequest(1L, 1L, 1L, "woven-nimbus-130608", table, columns, params, new HashMap<>()));
        log.info("Count: \n\t{}", response.getCount());
        log.info("Data: \n\t{}", response.getData());
    }

    @Test
    @Ignore
    public void pivotQueryTest(){

        List<Show> select = ImmutableList.of(new Show("customer_price", Op.SUM));
        List<Agg> groupByRows = ImmutableList.of(new Agg("provider", null, new AggSettings(new AggSort(SortDirection.DESC, false, select.get(0).key(), Arrays.asList("US")))));
        List<Agg> groupByCols = ImmutableList.of(new Agg("country_code", null, new AggSettings(new AggSort(SortDirection.DESC, true))));

        QueryParams params = new QueryParams();
        params.setShows(select);
        params.setAggs(groupByRows);
        params.setPivot(groupByCols);

        BigQueryRequestExecutor executor = new BigQueryRequestExecutor();
        executor.search(new SearchRequest(1L, 1L, 1L, "woven-nimbus-130608", "[fintest.m10]", new ArrayList<>(), params, new HashMap<>()));
    }
}

