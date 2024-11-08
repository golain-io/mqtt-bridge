package bridge

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

type option func(*MQTTBridge)

func WithMQTTClient(client mqtt.Client) option {
	return func(o *MQTTBridge) {
		o.mqttClient = client
	}
}

func WithConnectionTimeout(timeout time.Duration) option {
	return func(o *MQTTBridge) {
		o.timeout = timeout
	}
}

func WithLogger(logger *zap.Logger) option {
	return func(o *MQTTBridge) {
		o.logger = logger
	}
}
