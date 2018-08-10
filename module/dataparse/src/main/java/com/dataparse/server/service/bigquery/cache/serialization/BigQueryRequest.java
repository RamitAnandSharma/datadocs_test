package com.dataparse.server.service.bigquery.cache.serialization;

import com.dataparse.server.service.visualization.request.*;
import lombok.*;

import java.util.*;

@Getter
public class BigQueryRequest {

    private QueryRequest request;
    private String requestUID;
    private String query;
    private Integer pageNumber = null;
    private Long pageSize = null;
    private boolean useIntelliCache = true;

    private BigQueryRequest(){
    }

    public static class Builder {

        BigQueryRequest instance;

        public Builder() {
            this.instance = new BigQueryRequest();
            this.instance.requestUID = UUID.randomUUID().toString().replaceAll("-", "");
        }

        private Builder setQuery(String query){
            this.instance.query = query;
            return this;
        }

        public Builder setRequestUID(String requestUID){
            if(requestUID != null) {
                this.instance.requestUID = requestUID;
            }
            return this;
        }

        public Builder setPageSize(Long pageSize){
            this.instance.pageSize = pageSize;
            return this;
        }

        public Builder setPageNumber(Integer pageNumber){
            this.instance.pageNumber = pageNumber;
            return this;
        }

        private Builder setQueryRequest(QueryRequest request){
            this.instance.request = request;
            return this;
        }

        public Builder setUseIntelliCache(boolean useIntelliCache){
            this.instance.useIntelliCache = useIntelliCache;
            return this;
        }

        public BigQueryRequest build(){
            return instance;
        }
    }

    public static Builder builder(QueryRequest request, String query){
        return new Builder()
                .setQuery(query)
                .setQueryRequest(request);
    }

    public static BigQueryRequest of(String requestUID){
        BigQueryRequest request = new BigQueryRequest();
        request.requestUID = requestUID;
        return request;
    }

}
