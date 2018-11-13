package dispatcher

import (
	"github.com/knative/eventing/pkg/buses"
	"github.com/nats-io/go-nats-streaming"

	"go.uber.org/zap"

	eventingv1alpha1 "github.com/knative/eventing/pkg/apis/eventing/v1alpha1"
	"github.com/knative/eventing/pkg/provisioners/natss/stanutil"
	"github.com/knative/eventing/pkg/provisioners/natss/controller/clusterchannelprovisioner"
	"time"
)

// SubscriptionsSupervisor manages the state of NATS Streaming subscriptions
type SubscriptionsSupervisor struct {
	logger *zap.Logger

	receiver         *buses.MessageReceiver
	dispatcher       *buses.MessageDispatcher

	subscriptions     map[buses.ChannelReference]map[subscriptionReference]*stan.Subscription
}

func NewDispatcher(logger *zap.Logger) (*SubscriptionsSupervisor, error) {
	d := &SubscriptionsSupervisor {
		logger: logger,
		dispatcher: buses.NewMessageDispatcher(logger.Sugar()),
		subscriptions: make(map[buses.ChannelReference]map[subscriptionReference]*stan.Subscription),
	}
	d.receiver = buses.NewMessageReceiver(createReceiverFunction(d, logger.Sugar()), logger.Sugar())

	return d, nil
}

func createReceiverFunction(s *SubscriptionsSupervisor, logger *zap.SugaredLogger) func(buses.ChannelReference, *buses.Message) error {
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

func (s *SubscriptionsSupervisor) Start(stopCh <-chan struct{}) error {
	s.receiver.Run(stopCh)
	return nil
}

func (s *SubscriptionsSupervisor) UpdateSubscriptions(channel eventingv1alpha1.Channel) error {
	s.logger.Sugar().Infof("UpdateSubscriptions() for channel: %v", channel)
	if channel.Spec.Subscribable == nil || len(channel.Spec.Subscribable.Subscribers) == 0  {
		s.logger.Sugar().Infof("Empty subscriptions for channel: %v ; nothing to update ", channel)
		return nil
	}

	subscriptions := channel.Spec.Subscribable.Subscribers
	activeSubs := make(map[subscriptionReference]bool)

	cRef := buses.NewChannelReferenceFromNames(channel.Name, channel.Namespace)
	chMap, ok := s.subscriptions[cRef]
	if !ok {
		chMap = make(map[subscriptionReference]*stan.Subscription)
		s.subscriptions[cRef] = chMap
	}
	for _, sub := range subscriptions {
		// check if the subscribtion already exist and do nothing in this case
		if _, ok := chMap[newSubscriptionReference(sub)]; ok {
			activeSubs[newSubscriptionReference(sub)] = true
			s.logger.Sugar().Infof("Subscription: %v already active for channel: %v", sub, cRef)
			continue
		}
		// subscribe
		if natssSub, err := s.subscribe(cRef, newSubscriptionReference(sub)); err != nil {
			return err
		} else {
			chMap[newSubscriptionReference(sub)] = natssSub
			activeSubs[newSubscriptionReference(sub)] = true
		}
	}
	// Unsubscribe for deleted subscriptions
	chMap, _ = s.subscriptions[cRef]
	for sub := range chMap {
		if ok := activeSubs[sub]; !ok {
			s.unsubscribe(cRef, sub)
		}
	}
	// delete the channel from s.subscriptions if chMap is empty
	if len(s.subscriptions[cRef]) == 0 {
		delete(s.subscriptions, cRef)
	}
	return nil
}

func (s *SubscriptionsSupervisor) subscribe(channel buses.ChannelReference, subscription subscriptionReference) (*stan.Subscription, error) {
	s.logger.Sugar().Infof("Subscribe() channel: %v; subscription: %+v", channel, subscription)

	mcb := func(msg *stan.Msg) {
		s.logger.Sugar().Infof("NATSS message received: %+v", msg)
		message := buses.Message{
			Headers: map[string]string{},
			Payload: []byte(msg.Data),
		}
		if err := s.dispatcher.DispatchMessage(&message, subscription.SubscriberURI, subscription.ReplyURI, buses.DispatchDefaults{}); err != nil {
			s.logger.Sugar().Warnf("Failed to dispatch message: %v", err)
			return
		}
		if err := msg.Ack(); err != nil {
			s.logger.Sugar().Warnf("Failed to acknowledge message: %v", err)
		}
	}
	var natsConn *stan.Conn
	var err error
	if natsConn, err = stanutil.GetNatssConnection(clusterchannelprovisioner.NatssUrl, clientId, s.logger.Sugar()); err != nil {
		s.logger.Sugar().Errorf("GetNatssConnection() failed: %v", err)
		return nil, err
	}
	// subscribe to a NATSS subject
	// TODO the channel name + namespace should be part of the subject
	ch := channel.Name + "." + channel.Namespace
	sub := subscription.Name + "." + subscription.Namespace
	if natssSub, err := (*natsConn).Subscribe(ch, mcb, stan.DurableName(sub), stan.SetManualAckMode(), stan.AckWait(1*time.Minute)); err != nil {
		s.logger.Sugar().Errorf(" Create new NATSS Subscription failed: %v", err)
		return nil, err
	} else {
		s.logger.Sugar().Infof("NATSS Subscription created: %+v", natssSub)
		return &natssSub, nil
	}
}

func (s *SubscriptionsSupervisor) unsubscribe(channel buses.ChannelReference, subscription subscriptionReference) error {
	s.logger.Info("Unsubscribing from channel", zap.Any("channel", channel), zap.Any("subscription", subscription))

	if stanSub, ok := s.subscriptions[channel][subscription]; ok {
		// delete from NATSS
		if err := (*stanSub).Unsubscribe(); err != nil {
			s.logger.Sugar().Errorf("error unsubscribing NATS Streaming subscription: %v", err)
		}
		delete(s.subscriptions[channel], subscription)
	}
	return nil
}