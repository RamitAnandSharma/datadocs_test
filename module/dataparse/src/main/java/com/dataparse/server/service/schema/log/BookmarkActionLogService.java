package com.dataparse.server.service.schema.log;

import lombok.extern.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.util.concurrent.*;

@Slf4j
@Service
public class BookmarkActionLogService {

    @Autowired
    private BookmarkActionLogRepository bigQueryBookmarkActionLogRepository;

    private ExecutorService taskExecutor;

    @PostConstruct
    public void init() {
        taskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void save(BookmarkActionLogEntry entry) {
        taskExecutor.submit(() -> bigQueryBookmarkActionLogRepository.save(entry));
    }

}
