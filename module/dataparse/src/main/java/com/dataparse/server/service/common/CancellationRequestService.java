package com.dataparse.server.service.common;

import com.dataparse.server.controllers.api.file.AbstractCancellationRequest;
import com.dataparse.server.service.hazelcast.HazelcastClient;
import com.google.common.base.Objects;
import com.hazelcast.core.ISet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Slf4j
@Service
public class CancellationRequestService {

    @Autowired
    private HazelcastClient hazelcastClient;

    private static final String SET_ID = "CANCELLATION_REQUESTS_SET";

    private ISet<Integer> getSet() {
        return hazelcastClient.getClient().getSet(SET_ID);
    }

    private Integer buildHashCode(Long userId, AbstractCancellationRequest request) {
        return Objects.hashCode(userId, request);
    }

    public void add(Long userId, AbstractCancellationRequest request) {
        getSet().add(buildHashCode(userId, request));
    }

    public boolean checkCanceledAndRemove(Long userId, AbstractCancellationRequest request) {
        return getSet().remove(buildHashCode(userId, request));
    }

    public boolean remove(Long userId, AbstractCancellationRequest request) {
        return getSet().remove(buildHashCode(userId, request));
    }

}
