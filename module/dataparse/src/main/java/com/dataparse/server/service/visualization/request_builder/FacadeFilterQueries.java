package com.dataparse.server.service.visualization.request_builder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FacadeFilterQueries {
    private String listFilterQuery;
    private String rangeFilterQuery;

    public Boolean isListFilterQueryExists() {
        return StringUtils.isNotBlank(this.listFilterQuery);
    }

    public Boolean isRangeFilterQueryExists() {
        return StringUtils.isNotBlank(this.rangeFilterQuery);
    }
}
