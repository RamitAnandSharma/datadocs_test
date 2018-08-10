package com.dataparse.server.config;

import com.dataparse.server.util.JsonUtils;
import com.dataparse.server.websocket.interceptors.ConnectionInterceptor;
import com.dataparse.server.websocket.interceptors.SubscriptionInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

import java.util.List;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry stompEndpointRegistry) {
        stompEndpointRegistry.addEndpoint("/websocket").withSockJS()
                .setInterceptors(new HttpSessionHandshakeInterceptor());
    }

    @Override
    public void configureWebSocketTransport(final WebSocketTransportRegistration registration) {
        super.configureWebSocketTransport(registration);
        registration.setMessageSizeLimit(65536 * 4);
        registration.setSendBufferSizeLimit(65536 * 4);
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/user", "/all");
        registry.setApplicationDestinationPrefixes("/websocket");
    }
    @Override
    public void configureClientOutboundChannel(ChannelRegistration registration) {
        super.configureClientOutboundChannel(registration);
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        super.configureClientInboundChannel(registration);
        // receiving messages is sometimes out of order,
        // so we have to reduce pool size to 1
        // https://stackoverflow.com/questions/29689838/sockjs-receive-stomp-messages-from-spring-websocket-out-of-order/40496644
        // todo think of other possible solutions
        registration.taskExecutor().corePoolSize(1);
        registration.setInterceptors(new SubscriptionInterceptor(), sockJSConnectionInterceptor());
    }

    @Override
    public boolean configureMessageConverters(List<MessageConverter> messageConverters) {
        MappingJackson2MessageConverter jacksonConverter = new MappingJackson2MessageConverter();
        jacksonConverter.setObjectMapper(JsonUtils.mapper);
        messageConverters.add(jacksonConverter);
        return false;
    }

    @Bean
    public ConnectionInterceptor sockJSConnectionInterceptor() {
        return new ConnectionInterceptor();
    }
}