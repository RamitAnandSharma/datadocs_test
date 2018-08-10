package com.dataparse.server.cache;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.service.bigquery.cache.*;
import com.dataparse.server.service.bigquery.cache.serialization.*;
import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.service.visualization.request.*;
import org.junit.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class IntelliCacheTest extends IsolatedContextTest {

    private HazelcastQueryCache getHazelcastQueryCacheMock(Map<String, BigQueryResult> cache){
        return new HazelcastQueryCache() {

            Map<String, BigQueryResult> instance = cache;

            @Override
            public void put(final String key, final BigQueryResult response) {
                instance.put(key, response);
            }

            @Override
            public BigQueryResult get(final String key) {
                return instance.get(key);
            }

            @Override
            public void evict(final String externalId) {
            }
        };
    }

    private IntelliCache createIntelliCache(){
        return createIntelliCache(new HashMap<>());
    }

    private IntelliCache createIntelliCache(Map<String, BigQueryResult> internalCache){

        IntelliCache cache = new IntelliCache();
        try {
            Field field = IntelliCache.class.getDeclaredField("queryCache");
            field.setAccessible(true);
            field.set(cache, getHazelcastQueryCacheMock(internalCache));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        return cache;
    }

    private QueryParams generateQueryParams(){
        QueryParams params = new QueryParams();
        params.getShows().add(new Show("a", Op.MAX));
        params.getShows().add(new Show("b", Op.SUM));
        params.getShows().add(new Show("c", Op.UNIQUE_COUNT));
        params.getAggs().add(new Agg("d"));
        params.getAggs().add(new Agg("e"));
        params.getPivot().add(new Agg("f"));
        params.getPivot().add(new Agg("g"));
        params.setSearch("search");
        params.getFilters().add(new Filter("a", Arrays.asList(new FilterValue(true, true, 1, 10L), new FilterValue(true, true, 2, 5L))));
        params.getFilters().add(new IntegerRangeFilter("b", 10L, 100L, 0L));
        params.getFilters().add(new DateRangeFilter("c", 1L, 10L, 0L, FixedDateType.year_to_date));
        return params;
    }

    private SearchRequest createRequest(QueryParams params){
        return new SearchRequest(1L, 1L, 1L, "1", "1", null, params, new HashMap<>());
    }

    @Test
    @Ignore
    public void cacheTest() throws Exception {
        IntelliCache cache = createIntelliCache();
        QueryParams params = generateQueryParams();
        BigQueryRequest request1 = BigQueryRequest.builder(createRequest(params), "original-query")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        cache.put(QueryKey.of(request1), BigQueryRawResult.of(null, 100, null));

        BigQueryResult result;

        QueryParams params2 = params.copy();
        params2.getShows().add(params2.getShows().remove(0));
        params2.getFilters().add(params2.getFilters().remove(0));
        BigQueryRequest request2 = BigQueryRequest.builder(createRequest(params2), "query-variation1")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request2));
        assertNotNull(result);

        QueryParams params3 = params.copy();
        params3.getAggs().add(params3.getAggs().remove(0));
        BigQueryRequest request3 = BigQueryRequest.builder(createRequest(params3), "query-variation2")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request3));
        assertNull(result);

        QueryParams params4 = params.copy();
        List<FilterValue> filterValues = new ArrayList<>(params4.getFilters().get(0).getList());
        filterValues.add(filterValues.remove(0));
        filterValues.get(0).setDocCount(100L);
        filterValues.add(new FilterValue(false, true, 3, 1L));
        params4.getFilters().get(0).setList(filterValues);
        BigQueryRequest request4 = BigQueryRequest.builder(createRequest(params4), "query-variation3")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request4));
        assertNotNull(result);

        QueryParams params5 = params.copy();
        filterValues = new ArrayList<>(params5.getFilters().get(0).getList());
        filterValues.add(new FilterValue(true, true, 3, 1L));
        params5.getFilters().get(0).setList(filterValues);
        BigQueryRequest request5 = BigQueryRequest.builder(createRequest(params5), "query-variation4")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request5));
        assertNull(result);

        QueryParams params6 = params.copy();
        BigQueryRequest request6 = BigQueryRequest.builder(createRequest(params6), "query-variation5")
                .setPageNumber(1)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request6));
        assertNull(result);

        QueryParams params7 = params.copy();
        BigQueryRequest request7 = BigQueryRequest.builder(createRequest(params7), "query-variation6")
                .setPageNumber(0)
                .setPageSize(100L)
                .build();
        result = cache.get(QueryKey.of(request7));
        assertNull(result);

        QueryParams params8 = params.copy();
        SearchRequest r = createRequest(params8);
        r.setAccountId("2");
        BigQueryRequest request8 = BigQueryRequest.builder(r, "query-variation7")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request8));
        assertNull(result);

        QueryParams params9 = params.copy();
        params9.getShows().get(0).getSettings().setSort(new Sort(SortDirection.DESC));
        BigQueryRequest request9 = BigQueryRequest.builder(createRequest(params9), "query-variation8")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        result = cache.get(QueryKey.of(request9));
        assertNull(result);
    }

    @Test
    public void evictionTest() {
        Map<String, BigQueryResult> internalCache = new HashMap<>();
        IntelliCache cache = createIntelliCache(internalCache);

        QueryParams params = generateQueryParams();
        BigQueryRequest request1 = BigQueryRequest.builder(createRequest(params), "q1")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        QueryKey key = QueryKey.of(request1);
        cache.put(key, BigQueryRawResult.of(null, 100, null));

        BigQueryResult result;
        result = cache.get(key);
        Assert.assertNotNull(result);

        cache.evict(key.getExternalId());
        internalCache.clear(); // should clean internal cache manually to test cache wrapper

        result = cache.get(key);
        Assert.assertNull(result);
    }

    @Test
    @Ignore
    public void loadTest() throws Exception {

        IntelliCache cache = createIntelliCache();

        QueryParams params = generateQueryParams();
        long start = System.currentTimeMillis();
        AtomicLong minPutTime = new AtomicLong(Long.MAX_VALUE),
                maxPutTime = new AtomicLong(0),
                avgPutTime = new AtomicLong(0);

        Runtime runtime = Runtime.getRuntime();
        long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used Memory before: " + usedMemoryBefore / 1024 / 1024);

        int total = 1000, users = 20;
        CountDownLatch latch = new CountDownLatch(users);
        for(int j = 0; j < users; j++) {
            int user = j;
            Thread t = new Thread(() -> {
                long putTimeStart;
                for (int i = 0; i < total; i++) {
                    QueryParams paramsCopy = params.copy();
                    paramsCopy.setSearch("search" + i);
                    SearchRequest r = new SearchRequest((long) user, (long) user, (long) user,
                                                        "1", String.valueOf(user),
                                                        null, paramsCopy, new HashMap<>());
                    BigQueryRequest request1 = BigQueryRequest.builder(r, String.valueOf(user) + "_" + String.valueOf(i))
                            .setPageNumber(0)
                            .setPageSize(1000L)
                            .build();
                    QueryKey key = QueryKey.of(request1);
                    putTimeStart = System.nanoTime();
                    cache.put(key, BigQueryRawResult.of(null, 100, null));
                    long putTime = (System.nanoTime() - putTimeStart);
                    minPutTime.updateAndGet(operand -> Math.min(operand, putTime));
                    maxPutTime.updateAndGet(operand -> Math.max(operand, putTime));
                    avgPutTime.updateAndGet(operand -> operand + putTime / total);
                    if (i % 100 == 0) {
                        System.out.println("User " + user + ": created " + i);
                    }
                }
                latch.countDown();
            });
            t.start();
        }
        latch.await();
        System.out.println("Generated in " + (System.currentTimeMillis() - start)
                           + ",\n\tavg put time: " + avgPutTime.get() / 1E6
                           + ",\n\tmin put time: " + minPutTime.get() / 1E6
                           + ",\n\tmax put time: " + maxPutTime.get() / 1E6);

        QueryParams params2 = params.copy();
        params2.setSearch("search" + (total - 1));
        SearchRequest r = new SearchRequest((long) users - 1, (long) users - 1, (long) users - 1,
                                            "1", String.valueOf(users - 1),
                                            null, params2, new HashMap<>());
        BigQueryRequest request2 = BigQueryRequest.builder(r, "-1")
                .setPageNumber(0)
                .setPageSize(1000L)
                .build();
        QueryKey key2 = QueryKey.of(request2);
        start = System.currentTimeMillis();
        BigQueryResult result = cache.get(key2);
        System.out.println((result == null ? "Not Found" : "Found") + ", time: " + (System.currentTimeMillis() - start) + "ms");
    }
}
