package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/google/uuid"
	"github.com/gurre/shardcoordinator"
	"github.com/gurre/shardcoordinator/route53lock"
)

type appConfig struct {
	hostedZoneID  string
	domainName    string
	recordPrefix  string
	shardID       string
	leaseDuration time.Duration
	renewPeriod   time.Duration
	workInterval  time.Duration
	verifyOnly    bool
}

func parseFlags() appConfig {
	cfg := appConfig{}

	flag.StringVar(&cfg.hostedZoneID, "hosted-zone-id", "", "Route53 hosted zone ID (required)")
	flag.StringVar(&cfg.domainName, "domain-name", "", "Base domain name (required)")
	flag.StringVar(&cfg.recordPrefix, "record-prefix", "lock", "DNS prefix for lock records")
	flag.StringVar(&cfg.shardID, "shard-id", "demo-shard", "Logical shard identifier")
	flag.DurationVar(&cfg.leaseDuration, "lease-duration", 15*time.Second, "Leadership lease duration")
	flag.DurationVar(&cfg.renewPeriod, "renew-period", 5*time.Second, "Renewal attempt frequency")
	flag.DurationVar(&cfg.workInterval, "work-interval", 2*time.Second, "Simulated work interval")
	flag.BoolVar(&cfg.verifyOnly, "verify-only", false, "Verify setup and exit")

	flag.Parse()

	if cfg.hostedZoneID == "" {
		log.Fatalf("--hosted-zone-id is required")
	}
	if cfg.domainName == "" {
		log.Fatalf("--domain-name is required")
	}
	if cfg.leaseDuration <= 0 {
		log.Fatalf("--lease-duration must be positive")
	}
	if cfg.renewPeriod <= 0 {
		log.Fatalf("--renew-period must be positive")
	}
	if cfg.workInterval <= 0 {
		log.Fatalf("--work-interval must be positive")
	}
	if cfg.recordPrefix == "" {
		log.Fatalf("--record-prefix cannot be empty")
	}
	if cfg.shardID == "" {
		log.Fatalf("--shard-id cannot be empty")
	}

	return cfg
}

func verifySetup(ctx context.Context, cfg appConfig, r53Client *route53.Client) {
	log.Printf("[INFO] Verifying Route53 hosted zone: %s", cfg.hostedZoneID)

	getZoneInput := &route53.GetHostedZoneInput{
		Id: &cfg.hostedZoneID,
	}
	zoneOutput, err := r53Client.GetHostedZone(ctx, getZoneInput)
	if err != nil {
		log.Fatalf("Hosted zone %s not found or inaccessible: %v", cfg.hostedZoneID, err)
	}

	log.Printf("[INFO] Found hosted zone: %s", *zoneOutput.HostedZone.Name)

	listRecordsInput := &route53.ListResourceRecordSetsInput{
		HostedZoneId: &cfg.hostedZoneID,
		MaxItems:     int32Ptr(1),
	}
	_, err = r53Client.ListResourceRecordSets(ctx, listRecordsInput)
	if err != nil {
		log.Fatalf("IAM permissions insufficient. Required: route53:ListResourceRecordSets, route53:ChangeResourceRecordSets. Error: %v", err)
	}

	log.Printf("[INFO] IAM permissions verified")
}

func generateOwnerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	pid := os.Getpid()
	uid := uuid.New().String()

	return fmt.Sprintf("%s-%d-%s", hostname, pid, uid)
}

func setupSignalHandler(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		log.Println("[INFO] Shutdown signal received")
		cancel()
	}()
}

func int32Ptr(v int32) *int32 {
	return &v
}

func main() {
	cfg := parseFlags()

	log.Println("[INFO] Starting Route53 Leader Election Example")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	r53Client := route53.NewFromConfig(awsConfig)

	verifySetup(ctx, cfg, r53Client)

	if cfg.verifyOnly {
		log.Println("[INFO] Verification complete. Exiting.")
		return
	}

	ownerID := generateOwnerID()
	log.Printf("[INFO] Owner: %s", ownerID)
	log.Printf("[INFO] Shard: %s", cfg.shardID)

	lockRecordName := fmt.Sprintf("%s.%s.%s", cfg.recordPrefix, cfg.shardID, cfg.domainName)
	log.Printf("[INFO] Lock Record: %s", lockRecordName)

	lock, err := route53lock.New(route53lock.Config{
		Client:       r53Client,
		HostedZoneID: cfg.hostedZoneID,
		DomainName:   cfg.domainName,
		RecordPrefix: cfg.recordPrefix,
	})
	if err != nil {
		log.Fatalf("Failed to create Route53 lock: %v", err)
	}

	coordinator, err := shardcoordinator.New(shardcoordinator.Config{
		ShardID:       cfg.shardID,
		OwnerID:       ownerID,
		Locker:        lock,
		LeaseDuration: cfg.leaseDuration,
		RenewPeriod:   cfg.renewPeriod,
	})
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	setupSignalHandler(cancel)

	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := coordinator.Stop(stopCtx); err != nil {
			log.Printf("[ERROR] Failed to stop coordinator: %v", err)
		}
	}()

	wasLeader := false
	workCounter := 0

	ticker := time.NewTicker(cfg.workInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[INFO] Shutting down gracefully...")
			return
		case <-ticker.C:
			isLeader := coordinator.IsLeader()

			if isLeader && !wasLeader {
				log.Println("[LEADER] Acquired leadership!")
			} else if !isLeader && wasLeader {
				log.Println("[FOLLOWER] Lost leadership")
				workCounter = 0
			}

			if isLeader {
				workCounter++
				log.Printf("[LEADER] Processing work #%d", workCounter)
			} else {
				log.Println("[FOLLOWER] Waiting for leadership...")
			}

			wasLeader = isLeader
		}
	}
}
