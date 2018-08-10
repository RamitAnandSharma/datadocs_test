package com.dataparse.server.service.bigquery.account;

import com.dataparse.server.service.bigquery.*;
import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.upload.*;
import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import java.util.*;

@Slf4j
@Service
public class LeastTablesAccountSelectionStrategy implements IAccountSelectionStrategy {

    @Autowired
    private BigQueryClient bigquery;

    @Autowired
    private TableRepository tableRepository;

    @Override
    public String getAccount(final Descriptor descriptor) {
        List<String> accounts = bigquery.getAvailableAccounts();
        Map<String, Long> tablesPerAccount = tableRepository.getTablesPerAccount(accounts);
        Map.Entry<String, Long> acc = tablesPerAccount.entrySet().stream().min(Comparator.comparingLong(e -> e.getValue())).orElseGet(null);
        if(acc == null){
            throw new RuntimeException("Can't find account");
        }
        log.info("Tables per account: {}", tablesPerAccount);
        return acc.getKey();
    }
}
