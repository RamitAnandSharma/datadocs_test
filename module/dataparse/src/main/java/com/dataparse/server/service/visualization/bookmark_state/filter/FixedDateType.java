package com.dataparse.server.service.visualization.bookmark_state.filter;

public enum FixedDateType {
    last_7_days, last_14_days, last_28_days, last_30_days,
    today, yesterday, this_week, last_week,
    this_month, last_month, this_quarter, last_quarter,
    this_year, last_year, year_to_date, all_date_range

//    Possible fixed date types from utils.clj
//    Can be used without overriding it in the library (request-builder)

//    :last_7_days
//    :last_14_days
//    :last_28_days
//    :last_30_days
//    :yesterday
//    :today
//    :tomorrow
//    :last_week
//    :this_week
//    :next_week
//    :last_month
//    :this_month
//    :next_month
//    :last_quarter
//    :this_quarter
//    :next_quarter
//    :last_year
//    :next_year
//    :year_to_date
}
