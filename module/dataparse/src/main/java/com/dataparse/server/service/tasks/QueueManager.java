package com.dataparse.server.service.tasks;

import com.dataparse.server.service.es.index.IndexRequest;
import com.dataparse.server.service.flow.FlowExecutionRequest;
import org.springframework.stereotype.Service;

@Service
public class QueueManager
{

    public QueueType getQueue(AbstractRequest request) {
        if(request instanceof IndexRequest) {
            return QueueType.INDEX_QUEUE;
        } else if(request instanceof FlowExecutionRequest) {
            return QueueType.FLOW_QUEUE;
        }
        return QueueType.APP_QUEUE;
    }

    public enum QueueType
    {
        APP_QUEUE("app_queue", 16, 1),
        INDEX_QUEUE("index_queue", 2, 1),
        FLOW_QUEUE("flow_queue", 2, 1);

        private final String queueName;
        private final int concurrentConsumers;
        private final int prefetch;
        /**
         * @param name routing key
         * @param concurrentConsumers number of parallel tasks per worker
         */
        QueueType(String name, int concurrentConsumers, int prefetch)
        {
            this.queueName = name;
            this.concurrentConsumers = concurrentConsumers;
            this.prefetch = prefetch;
        }

        public int getConcurrentConsumers()
        {
            return concurrentConsumers;
        }

        public int getPrefetch() {
            return prefetch;
        }

        public String getQueueName()
        {
            return queueName;
        }
    }
}
