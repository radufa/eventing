/*
Copyright 2018 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package fanout provides an http.Handler that takes in one request and fans it out to N other
// requests, based on a list of Subscriptions. Logically, it represents all the Subscriptions to a
// single Knative Channel.
// It will normally be used in conjunction with multichannelfanout.Handler, which contains multiple
// fanout.Handlers, each corresponding to a single Knative Channel.
package fanout

import (
	"time"

	eventingduck "github.com/knative/eventing/pkg/apis/duck/v1alpha1"
	"github.com/knative/eventing/pkg/buses"
	"go.uber.org/zap"

	"github.com/knative/eventing/pkg/provisioners/natss/stanutil"
//	"github.com/knative/eventing/pkg/buses/natss"
	"github.com/knative/eventing/pkg/provisioners/natss/clusterchannelprovisioner"
//	"os"
//	"github.com/knative/eventing/pkg/controller/bus"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/knative/eventing/pkg/sidecar/fanout"
//	"github.com/knative/eventing/pkg/controller/eventing/subscription"
)

const (
	clientId            = "knative-natss-dispatcher"
)

func NewHandler(logger *zap.Logger, config fanout.Config) *fanout.Handler {
	h := fanout.NewHandler(logger, config)
	// The receiver function needs to point back at the handler itself, so set it up after
	// initialization.
	//handler.receiver = buses.NewMessageReceiver(createReceiverFunction(handler), logger.Sugar())
	h.Receiver = buses.NewMessageReceiver(createReceiverFunction2(h, logger.Sugar()), logger.Sugar())

	// TODO update subscribtions to NATSS
	for _, sub := range config.Subscriptions {
		if err := subscribe(h, sub, logger); err != nil {
			logger.Sugar().Errorf("Register subscription into NATSS failed: %v", err)
		}
	}

	return h
}

func createReceiverFunction2(f *fanout.Handler, logger *zap.SugaredLogger) func(buses.ChannelReference, *buses.Message) error {
	return func(channel buses.ChannelReference, m *buses.Message) error {
		logger.Infof("Received message from %q channel", channel.String())
		var natsConn *stan.Conn
		var err error
		if natsConn, err = stanutil.GetNatssConnection(clusterchannelprovisioner.NatssUrl, clientId, logger); err != nil {
			logger.Errorf("GetNatssConnection() failed: %v", err)
			return err
		}
		// publish to Natss
		ch := channel.Name +"." + channel.Namespace
		if err := stanutil.Publish(natsConn, ch, &m.Payload, logger); err != nil {
			logger.Errorf("Error during publish: %+v", err)
			return err
		}
		logger.Infof("Published [%s] : '%s'", channel.String(), m)
		return nil
	}
}

func subscribe(h *fanout.Handler, subscription eventingduck.ChannelSubscriberSpec, logger *zap.Logger) error {
	logger.Sugar().Infof("Subscribe %q ", subscription.SubscriberURI)

	mcb := func(msg *stan.Msg) {
		logger.Sugar().Infof("NATSS message received: %+v", msg)
		message := buses.Message{
			Headers: map[string]string{},
			Payload: []byte(msg.Data),
		}
		//if err := bus.dispatcher.DispatchMessage(subscription, &message); err != nil {
		if err := h.Dispatch(&message); err != nil {
			logger.Sugar().Warnf("Failed to dispatch message: %v", err)
			return
		}
		if err := msg.Ack(); err != nil {
			logger.Sugar().Warnf("Failed to acknowledge message: %v", err)
		}
	}
	var natsConn *stan.Conn
	var err error
	if natsConn, err = stanutil.GetNatssConnection(clusterchannelprovisioner.NatssUrl, clientId, logger.Sugar()); err != nil {
		logger.Sugar().Errorf("GetNatssConnection() failed: %v", err)
		return err
	}
	// subscribe to a NATSS subject
	// TODO the channel name + namespace should be part of the subject
	ch := "aloha" +"." + "default"  // TODO
	subs := subscription.Ref.Name + "." + subscription.Ref.Namespace
	if natsStreamingSub, err := (*natsConn).Subscribe(ch, mcb, stan.DurableName(subs), stan.SetManualAckMode(), stan.AckWait(1*time.Minute)); err != nil {
		logger.Sugar().Errorf(" Create new NATSS Subscription failed: %+v", err)
		return err
	} else {
		logger.Sugar().Infof("NATSS Subscription created: %+v", natsStreamingSub)
		//f.subscribers[subscription.Name] = &natsStreamingSub
	}
	return nil
}
