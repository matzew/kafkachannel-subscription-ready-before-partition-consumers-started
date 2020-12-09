module github.com/maschmid/kafkachannel-subscription-ready-before-partition-consumers-started

go 1.15

require (
	github.com/cloudevents/sdk-go/v2 v2.3.1
	k8s.io/api v0.18.8
	k8s.io/apimachinery v0.18.8
	knative.dev/eventing v0.18.6
	knative.dev/eventing-contrib v0.18.7
	knative.dev/pkg v0.0.0-20201026165741-2f75016c1368
	knative.dev/serving v0.18.3
)

replace (
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.2
	k8s.io/api => k8s.io/api v0.18.8
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.18.8
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.8
	k8s.io/client-go => k8s.io/client-go v0.18.8
)
