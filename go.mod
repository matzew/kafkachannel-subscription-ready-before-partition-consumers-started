module github.com/maschmid/kafkachannel-subscription-ready-before-partition-consumers-started

go 1.15

require (
	github.com/cloudevents/sdk-go/v2 v2.4.1
	k8s.io/api v0.20.7
	k8s.io/apimachinery v0.20.7
	knative.dev/eventing v0.24.3
	knative.dev/eventing-kafka v0.24.5
	knative.dev/pkg v0.0.0-20210902173607-953af0138c75
	knative.dev/serving v0.24.1
)
