package com.example.mqttdevice;

import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@MessagingGateway(defaultRequestChannel = "mqttOutboundChannel")
public interface MqttGateway {
    void sendToMqtt(String data, @Header(MqttHeaders.TOPIC) String topic);
}

@Component
@MessagingGateway(defaultRequestChannel = "mqttHumidityOutboundChannel")
interface MqttHumidityGateway {
    void sendToMqtt(String data, @Header(MqttHeaders.TOPIC) String topic);
}

@Component
@MessagingGateway(defaultRequestChannel = "mqttTemperatureOutboundChannel")
interface MqttTemperatureGateway {
    void sendToMqtt(String data, @Header(MqttHeaders.TOPIC) String topic);
}
