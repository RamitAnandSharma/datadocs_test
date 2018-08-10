package com.dataparse.server.service.tasks;

import com.dataparse.server.service.tasks.scheduled.ScheduledJob;
import com.dataparse.server.util.SystemUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Service
public class ScheduledTaskService {

    private static final String ENABLE_SCHEDULER = "ENABLE_SCHEDULER";

    @Autowired
    ApplicationContext applicationContext;

    private boolean enabled;

    private ThreadPoolTaskScheduler executor;

    private final Map<ScheduledJob, ScheduledFuture> jobs = new HashMap<>();

    @PostConstruct
    public void init() {
        enabled = SystemUtils.getProperty(ENABLE_SCHEDULER, false);
        if(enabled) {
            executor = new ThreadPoolTaskScheduler();
            executor.initialize();
        } else {
            log.warn("Scheduler is disabled. To enable scheduler set environment variable ENABLE_SCHEDULER to true.");
        }
    }

    public void cancel(ScheduledJob job) {
        if(enabled) {
            if (jobs.containsKey(job)) {
                log.info("Cancelled job: " + jobs.get(job));
                jobs.remove(job).cancel(true);
            }
        }
    }

    public void schedule(ScheduledJob job) {
        if(enabled) {
            cancel(job);
            try {
                applicationContext.getAutowireCapableBeanFactory().autowireBeanProperties(job, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, false);
            } catch (Exception e) {
                log.error("Failed to autowire beans", e);
                return;
            }
            jobs.put(job, executor.schedule(job, new CronTrigger(job.getCronExpression(), job.getTimeZone())));
            log.info("Scheduled new job: " + job);
        }
    }

    @PreDestroy
    public void destroy(){
        if(enabled) {
            executor.shutdown();
        }
    }

}
