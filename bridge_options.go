package bridge

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

type option func(*MQTTNetBridge)

func WithMQTTClient(client mqtt.Client) option {
	return func(o *MQTTNetBridge) {
		o.mqttClient = client
	}
}

func WithLogger(logger *zap.Logger) option {
	return func(o *MQTTNetBridge) {
		o.logger = logger
	}
}

func WithRootTopic(rootTopic string) func(*MQTTNetBridge) {
	return func(b *MQTTNetBridge) {
		b.rootTopic = rootTopic
	}
}

func WithQoS(qos byte) func(*MQTTNetBridge) {
	return func(b *MQTTNetBridge) {
		b.qos = qos
	}
}
