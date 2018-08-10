package com.dataparse.server.service.tasks;


import com.dataparse.server.RestServer;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.util.FunctionUtils;
import com.dataparse.server.util.JsonUtils;
import com.dataparse.server.websocket.SockJSService;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.AtomicDouble;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TaskManagementService {

  @Autowired
  SockJSService sockJSService;

  @Autowired
  QueueManager queueManager;

  @Autowired
  ApplicationContext applicationContext;

  @Autowired
  TasksRepository tasksRepository;

  @Autowired
  UserRepository userRepository;

  private static final ConcurrentHashMap<String, Pair<Thread, AbstractTask>> RUNNING_TASKS = new ConcurrentHashMap<>();

  private CachingConnectionFactory cf;
  private RabbitAdmin admin;
  private TopicExchange exchange;

  private static String getAmqpURI() {
    return Optional.ofNullable(System.getenv("AMQP_URL")).orElse("amqp://guest:guest@localhost");
  }

  @PostConstruct
  public void init() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
    if (RestServer.isMaster()) {
      tasksRepository.dropNonFinishedTasks();
    }
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri(getAmqpURI());
    cf = new CachingConnectionFactory(connectionFactory);
    admin = new RabbitAdmin(cf);
    exchange = new TopicExchange("defaultExchange");
    admin.declareExchange(exchange);
    if (RestServer.isMaster()) {
      for (QueueManager.QueueType queueType : QueueManager.QueueType.values()) {
        admin.deleteQueue(queueType.getQueueName());
      }
    }
    for (QueueManager.QueueType queueType : QueueManager.QueueType.values()) {
      createQueue(queueType, true);
    }
  }

  private void createQueue(QueueManager.QueueType queueType, boolean autodelete) {
    if (admin.getQueueProperties(queueType.getQueueName()) == null) {
      log.info("Create queue " + queueType.getQueueName());
      Queue defaultTaskQueue = new Queue(queueType.getQueueName(), true, false, autodelete);
      admin.declareQueue(defaultTaskQueue);
      admin.declareBinding(BindingBuilder.bind(defaultTaskQueue).to(exchange).with(queueType.getQueueName()));
    }

    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cf);
    container.setConcurrentConsumers(queueType.getConcurrentConsumers());
    container.setMaxConcurrentConsumers(queueType.getConcurrentConsumers());
    container.setMessageListener(createListener());
    container.setPrefetchCount(queueType.getPrefetch());
    container.setQueueNames(queueType.getQueueName());
    container.start();
  }

  private MessageListenerAdapter createListener() {
    return new MessageListenerAdapter(new Object() {
      public void handleMessage(String task) {
        AbstractTask taskValue = JsonUtils.readValue(task, AbstractTask.class);
        Auth.set(taskValue.getRequest().getAuth());

        TaskInfo taskInfo = tasksRepository.getTaskInfo(taskValue.getId());

        TaskState currentState = taskValue.getLastState();
        Set<TaskState> s = taskValue.getStates().keySet();
        List<TaskState> states = s.stream()
            .sorted((ts1, ts2) -> Integer.valueOf(ts1.ordinal()).compareTo(ts2.ordinal()))
            .collect(Collectors.toList());

        try{
          applicationContext.getAutowireCapableBeanFactory().autowireBeanProperties(taskValue, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, false);
        } catch (Exception e){
          log.error("Failed to autowire beans", e);
          return;
        }

        for(TaskState state : states){
          if(currentState != null && currentState.ordinal() >= state.ordinal()) {
            continue;
          }
          currentState = state;
          break;
        }

        Integer retries = taskInfo.getStatistics().getStateRetries().get(currentState.name());
        if (retries == null) {
          retries = 0;
        }

        if (taskInfo.isStopFlag()){
          tasksRepository.setTaskStateFailed(taskValue.getId());
          log.warn("Task {} is stopped", taskValue.getId());
          taskValue.onTaskStopped(taskInfo);
          return;
        }

        if(currentState.ordinal() == 0 && retries == 0){
          try{
            taskValue.onBeforeStart(taskInfo);
          } catch (Exception e){
            log.error("On task before start exception", e);
          }
        }

        try {
          if (currentState.getMaxRetries() != -1
              && retries > currentState.getMaxRetries()) {
            taskInfo.setStopFlag(true);
            taskValue.setFinished(true);
            try {
              taskValue.onAfterFinish(taskInfo);
            } catch (Exception e) {
              log.info("Task finished step, resolved with errors.", e);
            }
            log.info("Task " + taskValue.getClass().getSimpleName() + " [" + taskValue.getId() + "] retries count (" + currentState.getMaxRetries() + ") is expired, cancelling...");
            return;
          }
          MDC.put("request", taskValue.getId());
          applicationContext.getAutowireCapableBeanFactory().autowireBeanProperties(taskValue, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, false);
          RUNNING_TASKS.put(taskInfo.getId(), Pair.of(Thread.currentThread(), taskValue));
          ((Runnable) taskValue.getStates().get(currentState)).run();
          if(states.indexOf(currentState) == states.size() - 1){
            taskValue.setFinished(true);
            Date start = taskInfo.getStatistics().getRequestReceivedTime();
            Date end = new Date();
            taskInfo.getStatistics().setRequestCompleteTime(end);
            taskInfo.getStatistics().setDuration(end.getTime() - start.getTime());
          }
        } catch (Exception e) {
          taskInfo.getStatistics().getStateRetries().put(currentState.name(), ++retries);
          if(e instanceof ExecutionException){ // exception was catched, determined and re-thrown
            taskInfo.getStatistics().setErrors(((ExecutionException) e).getErrors());
          } else { // unhandled exception
            taskInfo.getStatistics().setLastErrorMessage(ExceptionUtils.getMessage(e));
            taskInfo.getStatistics().setLastErrorStackTrace(ExceptionUtils.getStackTrace(e));
            if(ExceptionUtils.getRootCause(e) != null) {
              taskInfo
              .getStatistics()
              .setLastErrorRootCauseMessage(ExceptionUtils.getRootCause(e).getMessage());
            }
          }

          // interruption magic!
          if(Thread.currentThread().isInterrupted()){
            Thread.interrupted();
            taskValue.setFinished(true);
            taskValue.setInterrupted(true);
            taskInfo.getStatistics().setErrors(ExecutionException.of("interrupted", null).getErrors());
            log.info("Task interrupted: {}[id={}]", taskValue.getClass().getSimpleName(), taskValue.getId());
          } else {
            try {
              Thread.sleep(currentState.getTimeout());
            } catch (InterruptedException e1) {
              taskValue.setFinished(true);
            }
            throw e;
          }
        } finally {
          // persist statistics
          taskValue.setLastState(currentState);
          tasksRepository.updateTaskInfo(taskInfo.getId(), TaskInfo.update(taskValue, taskInfo));
          RUNNING_TASKS.remove(taskInfo.getId());
          MDC.clear();
        }
        if (!taskValue.isFinished()) {
          sendTask(taskValue);
        } else {
          try{
            taskValue.onAfterFinish(taskInfo);
          } catch (Exception e){
            log.error("On task after finish exception", e);
          }
        }
      }
    });
  }

  public String execute(Auth auth, AbstractRequest request) {
    Class taskClass = request.getTaskClass();
    if (taskClass == null) {
      throw new RuntimeException("Can't find task for request " + request.getClass());
    }
    AbstractTask task = null;
    try {
      task = (AbstractTask) taskClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    String id = UUID.randomUUID().toString();
    task.setId(id);
    request.setAuth(auth);

    task.init(request);
    request.setRoutingKey(queueManager.getQueue(request).getQueueName());

    TaskInfo taskInfo = TaskInfo.create(task);
    tasksRepository.saveTaskInfo(taskInfo);
    sendTask(task);
    return id;
  }

  public TaskInfo executeSync(Auth auth, AbstractRequest request) {
    String taskId = execute(auth, request);
    return waitUntilFinished(taskId);
  }

  public boolean stop(String taskId){
    return tasksRepository.setTaskStopFlag(taskId);
  }

  public Map<String, Long> getRunningTasksCount() {
    return RUNNING_TASKS.entrySet().stream()
        .map(entry -> entry.getValue().getRight().getClass().toString())
        .collect(Collectors.toList())
        .stream().collect(Collectors.groupingBy(FunctionUtils.identity(), Collectors.counting()));
  }

  @Scheduled(cron = "0/5 * * * * ?")
  private void checkStoppedTasks(){
    Set<String> stoppingTasks = tasksRepository.getStoppingTasks();
    for(String stoppingTask: stoppingTasks){
      try {
        Pair<Thread, AbstractTask> task = RUNNING_TASKS.get(stoppingTask);
        if(task != null){
          if(!task.getLeft().isInterrupted()) {
            task.getLeft().interrupt();
          }
          task.getRight().cancel();
        }
      } catch (Exception e){
        log.error("Error while stopping tasks", e);
      }
    }
  }

  public List<TaskInfo> waitUntilFinished(List<String> taskIds, Consumer<Double> progressCallback) {
    if (taskIds.isEmpty()) {
      return new ArrayList<>();
    }
    Retryer<Boolean> reportRetryer = RetryerBuilder.<Boolean>newBuilder()
        .retryIfResult(Predicates.equalTo(false))
        .withWaitStrategy(WaitStrategies.fixedWait(2, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.neverStop())
        .build();
    try {
      AtomicDouble lastCompleteProcessing = new AtomicDouble();
      reportRetryer.call(() -> {
        List<String> finishedTasks = tasksRepository.getFinishedTasks(taskIds);
        Double complete = (finishedTasks.size() / (double) taskIds.size()) * 100.;
        if (!complete.equals(lastCompleteProcessing.get())) {
          if (progressCallback != null) {
            progressCallback.accept(lastCompleteProcessing.get());
            lastCompleteProcessing.set(complete);
          }
        }
        return finishedTasks.size() == taskIds.size();
      });

      log.info("All tasks {} has finished...", taskIds);
    } catch (Exception e) {
      log.error("Error while pending for tasks status", e);
      throw new RuntimeException(e);
    }

    return tasksRepository.getTaskInfo(taskIds);
  }

  public List<TaskInfo> waitUntilFinished(List<String> taskIds) {
    return waitUntilFinished(taskIds, null);
  }

  public TaskInfo waitUntilFinished(String taskId) {
    List<TaskInfo> finished = waitUntilFinished(Collections.singletonList(taskId));
    if (finished.isEmpty()) {
      throw new IllegalStateException("Error while pending for tasks status");
    }
    return finished.get(0);
  }

  public void sendTask(AbstractTask task) {
    RabbitTemplate template = new RabbitTemplate(cf);
    template.convertAndSend("defaultExchange", task.routingKey(), JsonUtils.writeValue(task));
  }

  @Data
  public class TaskRetryerBuilder {

    private String taskId;
    private boolean fastFail;

    private TaskRetryerBuilder(final String taskId) {
      this.taskId = taskId;
    }

    public TaskRetryerBuilder fastFail(){
      this.fastFail = true;
      return this;
    }

    private void checkErrors(TaskInfo info){
      if(this.fastFail) {
        if (info.getStatistics().getErrors() != null) {
          throw ExecutionException.of(info.getStatistics().getErrors());
        }
        if (info.getStatistics().getLastErrorMessage() != null){
          throw new RuntimeException(info.getStatistics().getLastErrorRootCauseMessage());
        }
      }
    }

    public TaskInfo doOnce(){
      TaskInfo info = tasksRepository.getTaskInfo(taskId);
      checkErrors(info);
      return info;
    }

    public TaskInfo retry(Consumer<TaskInfo> callback){
      Retryer<TaskInfo> retryer = RetryerBuilder.<TaskInfo>newBuilder()
          .retryIfResult(info -> {
            callback.accept(info);
            return info != null && !info.isFinished();
          })
          .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
          .withStopStrategy(StopStrategies.neverStop())
          .build();
      TaskInfo info;
      try {
        info = retryer.call(() -> tasksRepository.getTaskInfo(taskId));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      checkErrors(info);
      return info;
    }
  }

  public TaskRetryerBuilder check(String taskId){
    return new TaskRetryerBuilder(taskId);
  }
}
