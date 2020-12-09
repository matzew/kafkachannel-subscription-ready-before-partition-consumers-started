.PHONY = apply all clean

# List all subdirectories that we want to run "go build" on
images := sender receiver

targets := $(foreach dir,$(images),$(dir)/$(dir))

all: $(targets)

# ko apply
apply: all
	kubectl apply -f config/namespace.yaml
	ko apply -f config/receiver.yaml
	ko apply -f config/sender.yaml

%: %.go
	cd $(dir $@); go build

clean:
	rm -f $(targets)
