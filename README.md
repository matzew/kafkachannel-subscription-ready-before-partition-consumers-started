# kafkachannel-subscription-ready-before-partition-consumers-started
Reproducer for a knative eventing kafkachannel issue

TLDR:
* `make test`

Prerequisites:
* Make sure `ko` exists

To deploy, first build and deploy sender and receiver ksvcs (will be created in namespace "foobar")
(Note, the Sender ksvc will not succefully deploy, as it expects to be modified by SinkBinding created by the next step)
* `make apply`

Then run the test (which creates a KafkaChannel, SinkBinding and a Subscription, and invokes the sender to start sending events
once the channel's subscriber is Ready)
* `export SYSTEM_NAMESPACE=knative-eventing`
* `go test -v`

The test then invokes the sender ksvc to start producing events (1000 in total).

When the issue occurs, the first N events will not be received:

```
    reproducer_test.go:353: Event with ID 1 should be received exactly once, was 0
    reproducer_test.go:353: Event with ID 2 should be received exactly once, was 0
    reproducer_test.go:353: Event with ID 3 should be received exactly once, was 0
    reproducer_test.go:353: Event with ID 4 should be received exactly once, was 0
    reproducer_test.go:353: Event with ID 5 should be received exactly once, was 0
    reproducer_test.go:353: Event with ID 6 should be received exactly once, was 0
    reproducer_test.go:353: Event with ID 7 should be received exactly once, was 0
...
```

TestDispatcherRestartBeforeCommitedEvents is a variant of the same test, but it also restarts (deletes pod of) kafka-ch-dispatcher
once the Subscription is ready.
