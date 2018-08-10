package com.dataparse.server.service.share;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.cache.ICacheEvictionService;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.files.UserFileShare;
import com.dataparse.server.service.user.User;
import com.dataparse.server.util.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import javax.transaction.Transactional;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class ShareRepository {

    private final String REVOKE_PERMISSIONS_QUERY = "REVOKE_PERMISSIONS";

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TemplateEngine templateEngine;

    @Autowired
    private ICacheEvictionService cacheEvictionService;

    @Transactional
    public List<String> getRevokePermissionsUsers(Long datadocId, Long userForRevoke) {
        Context ctx = new Context();
        ctx.setVariable("userForRevoke", userForRevoke);
        ctx.setVariable("datadocId", datadocId);
        String query = templateEngine.process(REVOKE_PERMISSIONS_QUERY, ctx);
        List result = sessionFactory.getCurrentSession().createNativeQuery(query).list();
        return (List<String>) result.stream().map((arr) -> ((Object[]) arr)[2]).collect(Collectors.toList());
    }

    @Transactional
    public void cleanUpSharing(Long datadocId) {
        String query = "DELETE FROM UserFileShare u where u.primaryKey.datadoc.id = :datadocId";

        sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("datadocId", datadocId)
                .executeUpdate();

        cacheEvictionService.findKeyByPartialKey(datadocId.toString())
                .forEach(cachedKey -> cacheEvictionService.evict(cachedKey));
    }

    @Transactional
    // TODO: Simplify query
    // TODO: Check if cacheEvictionService can be replaced by @CacheEvict with (allEntries = true)
    @Cacheable(value = "shareWithUsers", key = "#datadocId.toString() + #userId.toString() + (#namePart ?: \"\")", unless = "#result.isEmpty()")
    public List<User> getShareWithUsers(Long datadocId, Long userId, String namePart) {
        String queryString = "SELECT u FROM User u WHERE u.id IN (SELECT DISTINCT " +
                "CASE WHEN s.primaryKey.user.id = :userId THEN s.shareFrom.id ELSE s.primaryKey.user.id END " +
                "FROM UserFileShare s WHERE s.primaryKey.user.id = :userId OR s.shareFrom.id = :userId) " +
                "AND u.id NOT IN (SELECT CASE WHEN cs.primaryKey.user.id = :userId THEN cs.shareFrom.id ELSE cs.primaryKey.user.id END FROM UserFileShare cs " +
                "WHERE cs.primaryKey.datadoc.id = :datadocId)"
                + (StringUtils.isEmpty(namePart) ? "" : " AND LOWER(u.email) LIKE '" + namePart.toLowerCase() + "%'");

        cacheEvictionService.addKeyToList(StringUtils.join(datadocId, userId, namePart));

        return sessionFactory
                .getCurrentSession()
                .createQuery(queryString)
                .setParameter("userId", userId)
                .setParameter("datadocId", datadocId)
                .setMaxResults(10)
                .list();
    }

    @Transactional
    public List<Datadoc> getSharedDatadocs(Long userId, Integer limit) {
        String query = "SELECT u FROM UserFileShare u where u.primaryKey.user.id = :user";

        Query q = sessionFactory
                .getCurrentSession()
                .createQuery(query)
                .setParameter("user", userId);
        if(limit != null) {
            q.setMaxResults(limit);
        }

        return ((List<UserFileShare> ) q.list()).stream().map(UserFileShare::getDatadoc).collect(Collectors.toList());
    }

    @Transactional
    public List<Datadoc> getSharedDatadocs(Long userId) {
        return getSharedDatadocs(userId, null);
    }

    /**
     * @return null if datadoc isn't shared
     * */
    @Transactional
    public ShareType getShareType(Long datadocId, Long userId) {
        UserFileShare sharedInfo = getSharedInfo(datadocId, userId);
        return sharedInfo == null ? null : sharedInfo.getShareType();
    }

    @Transactional
    public UserFileShare getSharedInfo(Long datadocId, Long userId) {
        String query = "SELECT u FROM UserFileShare u where u.primaryKey.user.id = :user and u.primaryKey.datadoc.id = :datadoc";
        List<UserFileShare> sharedFiles = sessionFactory
                .getCurrentSession()
                .createQuery(query)
                .setParameter("user", userId)
                .setParameter("datadoc", datadocId)
                .setMaxResults(1)
                .list();
        return ListUtils.first(sharedFiles);
    }


    @Transactional
    public void updatePermissions(UserFileShare userFileShare) {
        sessionFactory.getCurrentSession().update(userFileShare);
    }

    @Transactional
    public void revokePermissions(Long datadocId, Long userId) {
        String query = "DELETE FROM UserFileShare u where u.primaryKey.user.id = :user and u.primaryKey.datadoc.id = :datadoc";

        sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("user", userId)
                .setParameter("datadoc", datadocId)
                .executeUpdate();

        cacheEvictionService.findKeyByPartialKey(datadocId.toString() + Auth.get().getUserId().toString())
                .forEach(cachedKey -> cacheEvictionService.evict(cachedKey));
    }

    @Transactional
    public void saveShareDatadoc(UserFileShare userFileShare) {
        sessionFactory.getCurrentSession().saveOrUpdate(userFileShare);
    }

    @Transactional
    public List<UserFileShare> retrieveSharedUsers(Datadoc datadoc) {
        String query = "SELECT u FROM UserFileShare u where u.primaryKey.datadoc.id = :datadoc and u.owner.id = :owner";
        return sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("datadoc", datadoc.getId())
                .setParameter("owner", datadoc.getUser().getId())
                .list();
    }
}
