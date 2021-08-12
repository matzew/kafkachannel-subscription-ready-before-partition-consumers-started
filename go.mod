module github.com/maschmid/kafkachannel-subscription-ready-before-partition-consumers-started

go 1.15

require (
	github.com/cloudevents/sdk-go/v2 v2.4.1
	k8s.io/api v0.19.7
	k8s.io/apimachinery v0.19.7
	knative.dev/eventing v0.23.3
	knative.dev/eventing-kafka v0.23.5
	knative.dev/pkg v0.0.0-20210510175900-4564797bf3b7
	knative.dev/serving v0.23.1
)
