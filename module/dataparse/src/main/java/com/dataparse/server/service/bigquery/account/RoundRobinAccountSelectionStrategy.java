package com.dataparse.server.service.bigquery.account;

import com.dataparse.server.service.bigquery.*;
import com.dataparse.server.service.hazelcast.*;
import com.dataparse.server.service.upload.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import java.util.*;

public class RoundRobinAccountSelectionStrategy implements IAccountSelectionStrategy {

    private static final String ACCOUNT_COUNTER = "ACCOUNT_COUNTER";

    @Autowired
    private HazelcastClient hazelcast;

    @Autowired
    private BigQueryClient bigquery;

    @Override
    public String getAccount(final Descriptor descriptor) {
        List<String> accounts = bigquery.getAvailableAccounts();
        long count = hazelcast.getClient().getAtomicLong(ACCOUNT_COUNTER).getAndIncrement();
        return accounts.get((int) count % accounts.size());
    }
}
