package com.dataparse.server.service.schema.log;

import org.hibernate.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.transaction.*;
import java.util.*;

@Service
public class BookmarkActionLogRepository {

    @Autowired
    private SessionFactory sessionFactory;

    @Transactional
    public long getTotalBytesByUser(Long userId) {
        return (Long) sessionFactory
                .getCurrentSession()
                .createQuery("select coalesce(sum(e.bytesProcessed), 0) " +
                             "from RequestLogEntry e " +
                             "where e.userId = :userId and e.success = :isSuccess")
                .setParameter("isSuccess", true)
                .setParameter("userId", userId)
                .uniqueResult();
    }

    @Transactional
    public long getTotalBytesByBookmark(Long bookmarkId) {
        return (Long) sessionFactory
                .getCurrentSession()
                .createQuery("select coalesce(sum(e.bytesProcessed), 0) " +
                             "from RequestLogEntry e " +
                             "where e.bookmarkId = :bookmarkId and e.success = :isSuccess")
                .setParameter("isSuccess", true)
                .setParameter("bookmarkId", bookmarkId)
                .uniqueResult();
    }

    @Transactional
    public long getTotalBytes() {
        return (Long) sessionFactory
                .getCurrentSession()
                .createQuery("select coalesce(sum(e.bytesProcessed), 0) " +
                             "from RequestLogEntry e " +
                             "where e.success = :isSuccess")
                .setParameter("isSuccess", true)
                .uniqueResult();
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<BookmarkActionLogEntry> getUserEvents(Long userId){
        return sessionFactory
                .getCurrentSession()
                .createQuery("select e from BookmarkActionLogEntry e " +
                             "where e.userId = :userId " +
                             "order by e.startTime desc")
                .setParameter("userId", userId)
                .list();
    }

    @Transactional
    public void save(BookmarkActionLogEntry entry){
        sessionFactory.getCurrentSession().save(entry);
    }
}
