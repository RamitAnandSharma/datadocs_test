package com.dataparse.server.service.export;

import com.dataparse.server.service.storage.IStorageStrategy;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;
import java.util.UUID;

@Repository
public class ExportRepository {

    private static final Integer DAYS_UNTIL_EXPIRED = 7;

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private IStorageStrategy storageStrategy;

    @Transactional
    public void save(ExportEntity exportEntity) {
        sessionFactory.getCurrentSession().save(exportEntity);
    }

    @Transactional
    public List<ExportEntity> getExpired() {
        String query = "SELECT ee FROM ExportEntity ee where removed = :false and exportDate >= :thresholdDate";
        return sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("false", false)
                .setParameter("thresholdDate", new DateTime().minusDays(DAYS_UNTIL_EXPIRED).toDate())
                .list();
    }

    @Transactional
    public List<ExportEntity> getSendEmailsEntities() {
        String query = "SELECT ee FROM ExportEntity ee where removed = :false and downloaded = :false and emailSent = :false";
        return sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("false", false)
                .list();
    }



    @Transactional
    public void update(ExportEntity exportEntity) {
        sessionFactory.getCurrentSession().update(exportEntity);
    }

    @Transactional
    public void remove(UUID id) {
        storageStrategy.getDefault().removeFile(id.toString());
        String updateQuery = "UPDATE ExportEntity set removed = :true where resultFileId = :id";
        sessionFactory.getCurrentSession().createQuery(updateQuery)
                .setParameter("true", true)
                .setParameter("id", id)
                .executeUpdate();
    }



    @Transactional
    public void updateEmailSent(UUID id) {
        String updateQuery = "UPDATE ExportEntity set emailSent = :true where resultFileId = :id";
        sessionFactory.getCurrentSession().createQuery(updateQuery)
                .setParameter("true", true)
                .setParameter("id", id)
                .executeUpdate();
    }

    @Transactional
    public void updateDownloaded(UUID id, Boolean downloaded) {
        String updateQuery = "UPDATE ExportEntity set downloaded = :downloaded where resultFileId = :id";
        sessionFactory.getCurrentSession().createQuery(updateQuery)
                .setParameter("downloaded", downloaded)
                .setParameter("id", id)
                .executeUpdate();
    }


}
