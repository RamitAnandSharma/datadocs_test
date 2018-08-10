package com.dataparse.server.service.cleanup;

import com.dataparse.server.service.schema.TableRepository;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CleanUpService {

    @Autowired
    private TableRepository tableRepository;

    @Scheduled(cron = "0 0 12 * * ?")
    public void runCleanUp() {
        DateTime currentDate = new DateTime();
        int cleanUpByTimeCount = tableRepository.cleanUpPreSavedDatadocs(currentDate);
        log.info("Clean up {} pre saved datadocs.", cleanUpByTimeCount);
    }
}
