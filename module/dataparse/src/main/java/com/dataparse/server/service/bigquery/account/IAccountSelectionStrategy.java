package com.dataparse.server.service.bigquery.account;

import com.dataparse.server.service.upload.*;

public interface IAccountSelectionStrategy {

    String getAccount(Descriptor descriptor);

}
