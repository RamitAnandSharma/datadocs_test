package com.dataparse.server.service.visualization.bookmark_state;

import com.dataparse.server.auth.*;
import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.websocket.*;
import com.google.common.collect.*;
import lombok.extern.slf4j.*;
//import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.*;
import org.springframework.beans.factory.config.*;
import org.springframework.context.*;
import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@Slf4j
@Service
@DependsOn({"bookmarkStateRepository"})
public class BookmarkStateStorage {

    private final static int HISTORY_SIZE = 50;

    public static class StateHistory {

        private int maxSize = HISTORY_SIZE;

        private AtomicBoolean changed = new AtomicBoolean();
        private Stack<HistoryItem> toRedo = new Stack<>();
        private ArrayDeque<HistoryItem> toUndo = new ArrayDeque<>();

        public StateHistory(BookmarkState initialState){
            toUndo.push(new HistoryItem(initialState, null));
        }

        public void add(HistoryItem historyItem){
            if(toUndo.size() == maxSize){
                toUndo.removeLast();
            }
            toRedo.clear();
            toUndo.addFirst(historyItem);
        }

        private BookmarkStateHistoryWrapper wrap(HistoryItem historyItem){
            return new BookmarkStateHistoryWrapper(historyItem, toUndo.size() > 1, toRedo.size() > 0);
        }

        public BookmarkStateHistoryWrapper current(){
            return wrap(toUndo.peekFirst());
        }

        public BookmarkStateHistoryWrapper undo(){
            if(toUndo.size() == 1){
                throw new RuntimeException("Nothing to undo");
            }
            toRedo.push(toUndo.pollFirst());
            return current();
        }

        public BookmarkStateHistoryWrapper redo(){
            if(toRedo.size() == 0){
                throw new RuntimeException("Nothing to redo");
            }
            HistoryItem historyItem = toRedo.pop();
            toUndo.addFirst(historyItem);
            return current();
        }
    }

    @Autowired
    SockJSService sockJSService;

    @Autowired
    BookmarkStateRepository bookmarkStateRepository;

    @Autowired
    ApplicationContext applicationContext;

    private ConcurrentHashMap<BookmarkStateId, Queue<StateChangeEvent>> tabQueues = new ConcurrentHashMap<>();
    private ConcurrentHashMap<BookmarkStateId, StateHistory> tabStateHistoryMap = new ConcurrentHashMap<>();

    @PostConstruct
    protected void init(){
//        it's should be lazy
        for(BookmarkState state : bookmarkStateRepository.getBookmarkStateList()){
            BookmarkStateBuilder.repopulateDictionary(state);
            init(state, false);
        }
    }

    @PreDestroy
    protected void destroy(){
        flush();
    }

    @Scheduled(cron = "0/10 * * * * ?")
    private void flush(){
        for(StateHistory state: tabStateHistoryMap.values()){
            flush(state.current().getState().getBookmarkStateId());
            if(state.current().getState().getChangeable()) {
                saveBookmarkState(state);
            }
        }
    }

    private synchronized void saveBookmarkState(StateHistory state){
        if(state.changed.get()) {
            bookmarkStateRepository.save(state.current().getState());
            state.changed.set(false);
        }
    }

    private synchronized BookmarkState flush(BookmarkStateId stateId){
        StateHistory history = tabStateHistoryMap.get(stateId);
        if(history == null || history.current() == null) {
            log.warn("There is no data for bookmark state: {}", stateId);
        }
        BookmarkState state = history.current().getState();
        Queue<StateChangeEvent> queue = tabQueues.get(stateId);
        if(queue != null) {
            while (!queue.isEmpty()) {
                StateChangeEvent event = queue.poll();
                state = state.copy();
                try{
                    List<StateChangeEvent> eventsToAutowire = Lists.newArrayList(event);
                    if(event instanceof CompositeStateChangeEvent){
                        eventsToAutowire.addAll(((CompositeStateChangeEvent) event).getEvents());
                    }
                    for(StateChangeEvent eventToAutowire : eventsToAutowire){
                        applicationContext.getAutowireCapableBeanFactory()
                                .autowireBeanProperties(eventToAutowire, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, false);
                    }
                    event.apply(state);
                    // redirect event to other client instances
                    sockJSService.send(Auth.get(), String.format("/vis/event-response/%s/%s", state.getTabId(), stateId.getStateId()), event);
                } catch (Exception e){
                    log.error("Failed to autowire state change event", e);
                }
                history.changed.set(true);
                tabStateHistoryMap.get(stateId).add(new HistoryItem(state, event));
            }
        }
        return state;
    }

    public void init(BookmarkState state, boolean save){
        tabStateHistoryMap.put(state.getBookmarkStateId(), new StateHistory(state));
        if(save){
            bookmarkStateRepository.save(state);
        }
    }

    public BookmarkStateHistoryWrapper undo(BookmarkStateId stateId){
        flush(stateId);
        return tabStateHistoryMap.get(stateId).undo();
    }

    public BookmarkStateHistoryWrapper redo(BookmarkStateId stateId){
        flush(stateId);
        return tabStateHistoryMap.get(stateId).redo();
    }

    public void evict(BookmarkStateId stateId){
        tabStateHistoryMap.remove(stateId);
        tabQueues.remove(stateId);
        bookmarkStateRepository.remove(stateId);
    }

    public void evict(List<BookmarkStateId> states){
        states.stream().forEach(this::evict);
    }

    public BookmarkStateHistoryWrapper get(BookmarkStateId stateId, boolean forceFlush){
        return get(stateId, forceFlush, true);
    }

    public BookmarkStateHistoryWrapper get(BookmarkStateId stateId, boolean forceFlush, boolean fromMemory){
        if(fromMemory) {
            if(forceFlush){
                flush(stateId);
            }
            StateHistory stateHistory = tabStateHistoryMap.get(stateId);
            return stateHistory == null ? null : stateHistory.current();
        } else {
            return new BookmarkStateHistoryWrapper(bookmarkStateRepository.findOne(stateId));
        }

    }

    public void add(BookmarkStateChangeEvent event){
        log.info("Event arrived: {}, class = {}", event.getType().name(), event.getClass().getName());
        Long userId = Auth.get().getUserId() == null ? event.getUser() : Auth.get().getUserId();
        BookmarkStateId stateId = new BookmarkStateId(event.getTabId(), event.getStateId(), userId);
        Queue<StateChangeEvent> queue = tabQueues.computeIfAbsent(stateId, k -> new ConcurrentLinkedQueue<>());
        if (!queue.offer(event)) {
            throw new RuntimeException("Can't queue event");
        }
        flush(stateId);
    }

    public void setDefaultStateFromAnotherOne(BookmarkState stateFromCopy, UUID defaultStateId) {
        BookmarkStateId bookmarkState = new BookmarkStateId(stateFromCopy.getTabId(), defaultStateId, stateFromCopy.getUserId());
        StateHistory stateHistory = tabStateHistoryMap.get(bookmarkState);
        BookmarkState copy = stateFromCopy.copy();
        copy.setBookmarkStateId(bookmarkState);
        stateHistory.add(new HistoryItem(copy, new BookmarkVizCompositeStateChangeEvent()));
    }
}
