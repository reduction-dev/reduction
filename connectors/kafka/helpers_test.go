package kafka_test

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaCluster manages a test Kafka cluster in Docker and provides helpers.
type KafkaCluster struct {
	t          *testing.T
	BrokerAddr string
	client     *kgo.Client
	admin      *kadm.Client
}

// startKafka starts a Kafka docker container and returns a KafkaCluster.
func startKafka(t *testing.T) *KafkaCluster {
	containerName := "reduction-test-kafka"
	image := "apache/kafka-native:latest"
	brokerPort := "9092"
	zookeeperPort := "2181"

	exec.Command("docker", "rm", "-f", containerName).Run()

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"-p", brokerPort+":"+brokerPort,
		"-p", zookeeperPort+":"+zookeeperPort,
		image,
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to start kafka docker: %s", string(output))
	t.Cleanup(func() {
		exec.Command("docker", "rm", "-f", containerName).Run()
	})

	brokerAddr := "localhost:" + brokerPort
	client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	require.NoError(t, err, "failed to create kafka client")
	admin := kadm.NewClient(client)

	return &KafkaCluster{
		t:          t,
		BrokerAddr: brokerAddr,
		client:     client,
		admin:      admin,
	}
}

// CreateTopic creates a topic with the given name and 2 partitions.
func (k *KafkaCluster) CreateTopic(ctx context.Context, topic string) {
	_, err := k.admin.CreateTopics(ctx, 2, -1, nil, topic)
	require.NoError(k.t, err, "failed to create topic with admin client")
}

// Produce writes records to the topic and flushes.
func (k *KafkaCluster) Produce(ctx context.Context, records ...*kgo.Record) {
	for _, r := range records {
		err := k.client.ProduceSync(ctx, r).FirstErr()
		require.NoError(k.t, err, "produce record")
	}
	err := k.client.Flush(ctx)
	require.NoError(k.t, err, "flush records")
}

func (k *KafkaCluster) Close() {
	k.client.Close()
}

func integrationOnly(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" && os.Getenv("INTEGRATION_KAFKA") == "" {
		t.Skip("integration-only")
	}
}
