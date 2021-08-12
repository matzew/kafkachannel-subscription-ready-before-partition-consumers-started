package kafkachannel_subscription_ready_before_partition_consumers_started

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	kafkachannel "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	eventingcontribkafkachannelversioned "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	messaging "knative.dev/eventing/pkg/apis/messaging/v1"
	sources "knative.dev/eventing/pkg/apis/sources/v1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/pkg/apis"
	duck "knative.dev/pkg/apis/duck/v1"
	_ "knative.dev/pkg/system/testing"
	pkgTest "knative.dev/pkg/test"
	"knative.dev/pkg/tracker"
	servingversioned "knative.dev/serving/pkg/client/clientset/versioned"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	namespace = "foobar"
	sender = "sender"
	receiver = "receiver"
)

// TODO: use helpers
// Cannot use pkg test helpers due to flag conflicts
func appendRandomString(prefix string) string {
	return strings.Join([]string{prefix, randomString()}, "-")
}

// RandomString will generate a random string.
func randomString() string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"
	suffix := make([]byte, 8)

	for i := range suffix {
		suffix[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(suffix)
}

func prepareClients(t *testing.T) (*testlib.Client, *servingversioned.Clientset, *eventingcontribkafkachannelversioned.Clientset){
	client, err := testlib.NewClient(
		pkgTest.Flags.Kubeconfig,
		pkgTest.Flags.Cluster,
		namespace,
		t)
	if err != nil {
		t.Fatalf("Error creating Eventing test/lib Client: %v", err)
	}

	servingClient, err := servingversioned.NewForConfig(client.Config)
	if err != nil {
		t.Fatalf("Error creating Serving Client: %v", err)
	}

	kafkaClientSet, err := eventingcontribkafkachannelversioned.NewForConfig(client.Config)
	if err != nil {
		t.Fatalf("Error creating Kafka ClientSet: %v", err)
	}

	return client, servingClient, kafkaClientSet
}

func resetReceiver(t *testing.T, resetUrl string) {
	resp, err := http.Post(resetUrl, "text/plain", nil)
	if err != nil {
		t.Fatalf("error HTTP POST %s: %v", resetUrl, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status for HTTP POST %s: %s", resetUrl, resp.Status)
	}
}

// Returns a map of count of received messages, per "index", and the number of errors
// The format of the receiver is a CSV:
// index,count
// errors,<count>
// <index>,<count>
func getReceiverReport(t *testing.T, reportUrl string) map[int]int {
	resp, err := http.Get(reportUrl)
	if err != nil {
		t.Fatalf("error HTTP GET %s: %v", reportUrl, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status for HTTP GET %s: %s", reportUrl, resp.Status)
	}

	indexCounts := make(map[int]int)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ",")
		if len(split) == 2 {
			if split[0] == "id" && split[1] == "count" {
				// Header, ignore
			} else {
				index, err := strconv.Atoi(split[0])
				if err != nil {
					t.Errorf("unexpected format of line: %s", line)
				}

				count, err := strconv.Atoi(split[1])
				if err != nil {
					t.Errorf("unexpected format of an index %d count, line: %s", index, line)
				}

				indexCounts[index] = count
			}
		} else {
			t.Errorf("unexpected format of report output, line: %s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Errorf("error reading report: %v", err)
	}

	return indexCounts
}

// Hack for OpenShift router not being ready while ksvc already is
func waitForNo503(url string) error {
	const minConsecutiveNon503s = 5
	consecutiveNon503s := 0
	return wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		resp, err := http.Get(url)

		if err != nil {
			return false, err
		}

		defer resp.Body.Close()

		if resp.StatusCode == 503 {
			consecutiveNon503s = 0
		} else {
			consecutiveNon503s = consecutiveNon503s + 1
			if consecutiveNon503s >= minConsecutiveNon503s {
				return true, nil
			}
		}

		return false, nil
	})
}

func waitForKsvcReadiness(t *testing.T, servingClient *servingversioned.Clientset, namespace, name string) error {
	return wait.PollImmediate(1*time.Second, 5*time.Minute, func() (bool, error) {
		service, err := servingClient.ServingV1().Services(namespace).Get(context.Background(), name, meta.GetOptions{})
		if err != nil {
			return false, err
		}

		conditions := &service.Status.Conditions
		if conditions == nil {
			return false, nil
		}

		for _, condition := range *conditions {
			if condition.Type == apis.ConditionReady {
				if !condition.IsTrue() {
					t.Logf("Ksvc %s not Ready yet: %s", name, condition.Message)
					return false, nil
				} else {
					return true, nil
				}
			}
		}

		// no conditions yet
		return false, nil
	})
}

// Prepares receiver ksvc
func prepareReceiver(t *testing.T, servingClient *servingversioned.Clientset) string {
	// Wait until the "receiver" ksvc becomes Ready  (sender ksvc won't become ready until we create the SinkBinding)
	err := waitForKsvcReadiness(t, servingClient, namespace, receiver)
	if err != nil {
		t.Fatalf("Error waiting for ksvc %q readiness: %v", receiver, err)
	}
	// Get the receiver URL
	svc, err := servingClient.ServingV1().Services(namespace).Get(context.Background(), receiver, meta.GetOptions{})
	if err != nil {
		t.Fatalf("Error getting ksvc %q: %v", receiver, err)
	}
	receiverUrl := svc.Status.URL

	err = waitForNo503(receiverUrl.String())
	if err != nil {
		t.Fatalf("error waiting for the OpenShift route for ksvc %s", receiver)
	}

	// Reset the receiver (in case we're re-running the test)
	resetReceiver(t, receiverUrl.String()+"/reset")

	return receiverUrl.String()
}

func createChannel(t *testing.T, kafkaClientSet *eventingcontribkafkachannelversioned.Clientset, channelName string) {
	_, err := kafkaClientSet.MessagingV1beta1().KafkaChannels(namespace).Create(context.Background(), &kafkachannel.KafkaChannel{
		ObjectMeta: meta.ObjectMeta{
			Name: channelName,
		},
		Spec: kafkachannel.KafkaChannelSpec{
			NumPartitions:     8,
			ReplicationFactor: 3,
		},
	}, meta.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating KafkaChannel %q: %v", channelName, err)
	}
}

func prepareSender(t *testing.T, client *testlib.Client, servingClient *servingversioned.Clientset, channelName string) string {
	// Label the namespace for the SinkBinding
	_, err := client.Kube.CoreV1().Namespaces().Patch(context.Background(), namespace, types.StrategicMergePatchType, []byte("{ \"metadata\": { \"labels\": { \"bindings.knative.dev/include\": \"true\" } } }"), meta.PatchOptions{})
	if err != nil {
		t.Fatalf("error patching namespace %q: %v", namespace, err)
	}

	// Create SinkBinding for the Sender ksvc (we name it the same as the channel)
	_, err = client.Eventing.SourcesV1().SinkBindings(namespace).Create(context.Background(), &sources.SinkBinding{
		ObjectMeta: meta.ObjectMeta{
			Name: channelName,
		},
		Spec: sources.SinkBindingSpec{
			BindingSpec: duck.BindingSpec{
				Subject: tracker.Reference{
					APIVersion: "serving.knative.dev/v1",
					Kind:       "Service",
					Namespace:  namespace,
					Selector: &meta.LabelSelector{
						MatchLabels: map[string]string{
							"app": "sender",
						},
					},
				},
			},
			SourceSpec: duck.SourceSpec{
				Sink: duck.Destination{
					Ref: &duck.KReference{
						APIVersion: "messaging.knative.dev/v1beta1",
						Kind:       "KafkaChannel",
						Name:       channelName,
					},
				},
			},
		},
	}, meta.CreateOptions{})
	if err != nil {
		t.Fatalf("error creating SinkBinding %q: %v", channelName, err)
	}

	// Wait until the Sender ksvc becomed Ready
	err = waitForKsvcReadiness(t, servingClient, namespace, sender)
	if err != nil {
		t.Fatalf("Error waiting for ksvc %q readiness: %v", sender, err)
	}

	// Get the sender URL
	svc, err := servingClient.ServingV1().Services(namespace).Get(context.Background(), sender, meta.GetOptions{})
	if err != nil {
		t.Fatalf("Error getting ksvc %q: %v", sender, err)
	}
	senderUrl := svc.Status.URL

	// Waits until the sender OpenShift Route exists
	err = waitForNo503(senderUrl.String())
	if err != nil {
		t.Fatalf("error waiting for the OpenShift route for ksvc %s", sender)
	}

	return senderUrl.String()
}

// creates Subscription and wait until it's Ready (subscription is marked as ready in the Channel subscription list)
func prepareSubscription(t *testing.T, client *testlib.Client, kafkaClientSet *eventingcontribkafkachannelversioned.Clientset, subscriptionName string, channelName string) {
	// Create the Subscription
	_, err := client.Eventing.MessagingV1().Subscriptions(namespace).Create(context.Background(), &messaging.Subscription{
		ObjectMeta: meta.ObjectMeta{
			Name: subscriptionName,
		},
		Spec: messaging.SubscriptionSpec{
			Channel: core.ObjectReference{
				APIVersion: "messaging.knative.dev/v1beta1",
				Kind:       "KafkaChannel",
				Name:       channelName,
			},
			Subscriber: &duck.Destination{
				Ref: &duck.KReference{
					APIVersion: "serving.knative.dev/v1",
					Kind:       "Service",
					Name:       "receiver",
				},
			},
		},
	}, meta.CreateOptions{})

	if err != nil {
		t.Fatalf("Error creating Subscription: %v", err)
	}

	// Wait until the Channel is Ready and contains a Ready subscriber
	// We use frequent polling, so we can start immediately after the channel reports the subscriber is Ready
	err = wait.PollImmediate(100*time.Millisecond, 5*time.Minute, func() (bool, error) {
		channel, err := kafkaClientSet.MessagingV1beta1().KafkaChannels(namespace).Get(context.Background(), channelName, meta.GetOptions{})
		if err != nil {
			return false, err
		}

		conditions := &channel.Status.Conditions
		if conditions == nil {
			return false, nil
		}

		for _, condition := range *conditions {
			if condition.Type == apis.ConditionReady {
				if !condition.IsTrue() {
					t.Logf("Channel %s not Ready yet: %s", channelName, condition.Message)
					return false, nil
				}
			}
		}

		// We expect a single subscriber, so any Ready subscriber means "ready" for us.
		for _, subscriber := range channel.Status.Subscribers {
			t.Logf("Channel Subscriber: %v", subscriber)
			if subscriber.Ready == core.ConditionTrue {
				return true, nil
			}
		}

		return false, nil
	})

	if err != nil {
		t.Fatalf("Error waiting for channel %q readiness: %v", channelName, err)
	}
}

func sendEvents(t *testing.T, senderUrl string, count int) {
	t.Logf("Sending events...")
	// Invoke the sender (let it send `count` events, with 20ms interval between events)
	// The sender will retry if it cannot send an event
	resp, err := http.Post(fmt.Sprintf("%s/send?count=%d&interval=20ms", senderUrl, count), "text/plain", nil)
	if err != nil {
		t.Fatalf("Error invoking sender: %v", err)
	}

	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Error reading send request body: %v", err)
	}

	// Sender responds asynchronously with HTTP 202 once it starts sending events
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP POST to sender returned %v", resp.Status)
	}
}

func verifyEventsReceived(t *testing.T, receiverUrl string, total int) {
	counts := getReceiverReport(t, receiverUrl + "/report")
	// Every number between <1 and total> should be received exactly once
	for i := 1; i <= total; i++ {
		if counts[i] != 1 {
			t.Errorf("Event with ID %d should be received exactly once, was %d", i, counts[i])
		}
	}
}

func restartKafkaChannelDispatcher(t *testing.T, client *testlib.Client) {
	kafkaDispatcherList, err := client.Kube.CoreV1().Pods("knative-eventing").List(
		context.Background(),
		meta.ListOptions{LabelSelector: "messaging.knative.dev/channel=kafka-channel,messaging.knative.dev/role=dispatcher"})
	if err != nil {
		t.Fatal("Error listing knative-eventing kafka-ch-dispatcher pods", err)
	}

	for _, kafkaDispatcher := range kafkaDispatcherList.Items {
		t.Log("Deleting Pod", kafkaDispatcher.Name)
		err = client.Kube.CoreV1().Pods("knative-eventing").Delete(context.Background(), kafkaDispatcher.Name, meta.DeleteOptions{})
		if err != nil {
			t.Fatal("Error deleting", kafkaDispatcher.Name, err)
		}
	}
}

// restartKafka restarts one of the Kafka pods, it expects it exists in the "kafka" namespace
func restartKafka(t *testing.T, client *testlib.Client) {
	kafkaList, err := client.Kube.CoreV1().Pods("kafka").List(
		context.Background(),
		meta.ListOptions{LabelSelector: "app.kubernetes.io/name=kafka"})
	if err != nil {
		t.Fatal("Error listing kafka 'app.kubernetes.io/name=kafka' pods", err)
	}

	if len(kafkaList.Items) == 0 {
		t.Fatal("No 'app.kubernetes.io/name=kafka' pods in kafka namespace")
	}

	kafka := kafkaList.Items[0]
	t.Log("Deleting Pod", kafka.Name)
	err = client.Kube.CoreV1().Pods("kafka").Delete(context.Background(), kafka.Name, meta.DeleteOptions{})
	if err != nil {
		t.Fatal("Error deleting", kafka.Name, err)
	}
}

/*
TestSubscriptionReadyBeforeConsumerGroups expects that "make apply" was invoked before
(it uses the sender and receiver ksvcs created in "foobar" namespace by "make apply")

1. Wait for Subscription to be Ready
2. Sends 1000 events

*/
func TestSubscriptionReadyBeforeConsumerGroups(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// The issue seems to be happening only when the channel is "fresh", (it was not created before)
	// So we generate a random name for the channel and the subscription:
	subscriptionName := appendRandomString("subscription")
	channelName := appendRandomString("channel")

	client, servingClient, kafkaClientSet := prepareClients(t)

	receiverUrl := prepareReceiver(t, servingClient)

	// Create the KafkaChannel
	createChannel(t, kafkaClientSet, channelName)

	// Create the SinkBinding for the Sender and waits until Sender is Ready
	senderUrl := prepareSender(t, client, servingClient, channelName)

	// Creates Subscription and wait until it is Ready (and marked as ready in channel subscriber list)
	prepareSubscription(t, client, kafkaClientSet, subscriptionName, channelName)

	sendEvents(t, senderUrl, 1000)

	// 1000 messages with 20ms (50 events/s) => ~20 seconds, let's wait for two minutes to let all the events be delivered
	t.Logf("Sleeping for 2 minutes to let the events flow...")
	time.Sleep(2 * time.Minute)

	verifyEventsReceived(t, receiverUrl, 1000)
}


/*
TestDispatcherRestartBeforeCommitedEvents expects that "make apply" was invoked before
(it uses the sender and receiver ksvcs created in "foobar" namespace by "make apply")

1. Wait for Subscription to be Ready
2. Restarts kafka-ch-dispatcher
3. Sends 1000 events

*/
func TestDispatcherRestartBeforeCommitedEvents(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// The issue seems to be happening only when the channel is "fresh", (it was not created before)
	// So we generate a random name for the channel and the subscription:
	subscriptionName := appendRandomString("subscription")
	channelName := appendRandomString("channel")

	client, servingClient, kafkaClientSet := prepareClients(t)

	receiverUrl := prepareReceiver(t, servingClient)

	// Create the KafkaChannel
	createChannel(t, kafkaClientSet, channelName)

	// Create the SinkBinding for the Sender and waits until Sender is Ready
	senderUrl := prepareSender(t, client, servingClient, channelName)

	// Creates Subscription and wait until it is Ready (and marked as ready in channel subscriber list)
	prepareSubscription(t, client, kafkaClientSet, subscriptionName, channelName)

	// Delete the kafka-ch-dispatcher pod
	restartKafkaChannelDispatcher(t, client)

	sendEvents(t, senderUrl, 1000)

	// 1000 messages with 20ms (50 events/s) => ~20 seconds, let's wait for two minutes to let all the events be delivered
	t.Logf("Sleeping for two minutes to let the events flow...")
	time.Sleep(2 * time.Minute)

	verifyEventsReceived(t, receiverUrl, 1000)
}

/*
TestDispatcherRestartAfterCommitedEvents expects that "make apply" was invoked before
(it uses the sender and receiver ksvcs created in "foobar" namespace by "make apply")

1. Wait for Subscription to be Ready
2. Sends 1000 events
3. Restarts kafka-ch-dispatcher
4. Sends another 1000 events
*/
func TestDispatcherRestartAfterCommitedEvents(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// The issue seems to be happening only when the channel is "fresh", (it was not created before)
	// So we generate a random name for the channel and the subscription:
	subscriptionName := appendRandomString("subscription")
	channelName := appendRandomString("channel")

	client, servingClient, kafkaClientSet := prepareClients(t)

	receiverUrl := prepareReceiver(t, servingClient)

	// Create the KafkaChannel
	createChannel(t, kafkaClientSet, channelName)

	// Create the SinkBinding for the Sender and waits until Sender is Ready
	senderUrl := prepareSender(t, client, servingClient, channelName)

	// Creates Subscription and wait until it is Ready (and marked as ready in channel subscriber list)
	prepareSubscription(t, client, kafkaClientSet, subscriptionName, channelName)

	// Send 1st batch of events
	sendEvents(t, senderUrl, 1000)

	// 1000 messages with 20ms (50 events/s) => ~20 seconds, let's wait for a minute to let all the events be delivered
	t.Logf("Sleeping for a minute to let the events flow...")
	time.Sleep(1 * time.Minute)

	verifyEventsReceived(t, receiverUrl, 1000)

	// Delete the kafka-ch-dispatcher pod
	restartKafkaChannelDispatcher(t, client)

	// Send 2nd batch of events
	sendEvents(t, senderUrl, 1000)

	// 1000 messages with 20ms (50 events/s) => ~20 seconds, let's wait for two minutes to let all the events be delivered
	t.Logf("Sleeping for two minutes to let the events flow...")
	time.Sleep(2 * time.Minute)

	verifyEventsReceived(t, receiverUrl, 2000)
}


func TestKafkaRestart(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// The issue seems to be happening only when the channel is "fresh", (it was not created before)
	// So we generate a random name for the channel and the subscription:
	subscriptionName := appendRandomString("subscription")
	channelName := appendRandomString("channel")

	client, servingClient, kafkaClientSet := prepareClients(t)

	receiverUrl := prepareReceiver(t, servingClient)

	// Create the KafkaChannel
	createChannel(t, kafkaClientSet, channelName)

	// Create the SinkBinding for the Sender and waits until Sender is Ready
	senderUrl := prepareSender(t, client, servingClient, channelName)

	// Creates Subscription and wait until it is Ready (and marked as ready in channel subscriber list)
	prepareSubscription(t, client, kafkaClientSet, subscriptionName, channelName)

	// Send 1st batch of events
	sendEvents(t, senderUrl, 1000)

	// 1000 messages with 20ms (50 events/s) => ~20 seconds, let's wait for two minutes to let all the events be delivered
	t.Logf("Sleeping for two minutes to let the events flow...")
	time.Sleep(2 * time.Minute)

	verifyEventsReceived(t, receiverUrl, 1000)

	// Delete one of the Kafka pods
	restartKafka(t, client)

	// Send 2nd batch of events
	sendEvents(t, senderUrl, 1000)

	// 1000 messages with 20ms (50 events/s) => ~20 seconds, let's wait for two minutes to let all the events be delivered
	t.Logf("Sleeping for two minutes to let the events flow...")
	time.Sleep(2 * time.Minute)

	verifyEventsReceived(t, receiverUrl, 2000)
}
