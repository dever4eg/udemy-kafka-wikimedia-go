package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

func main() {
	godotenv.Load()

	ctx := context.Background()

	openSearchHosts := strings.Split(os.Getenv("OPENSEARCH_HOSTS"), ",")
	index := os.Getenv("OPENSEARCH_INDEX_WIKIMEDIA")

	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: openSearchHosts,
	})

	if err != nil {
		log.Fatal("Failed to connect to Opensearch")
	}

	err = EnsureIndexExists(ctx, index, client)
	if err != nil {
		log.Fatalf("Failed to create ad index: %s", err)
	}
}

func EnsureIndexExists(ctx context.Context, index string, client *opensearch.Client) error {
	existsReq := opensearchapi.IndicesExistsRequest{Index: []string{index}}
	existsResp, err := existsReq.Do(ctx, client)

	if err != nil {
		return err
	}

	if existsResp.StatusCode == 200 {
		log.Printf("Index exists: %s", index)

		return nil
	}

	log.Printf("Index not found. Creating index: %s", index)

	createReq := opensearchapi.IndicesCreateRequest{Index: index}
	createResp, err := createReq.Do(ctx, client)

	if err != nil {
		return err
	}

	if createResp.StatusCode != 200 {
		log.Printf("Failed to create index, response: %s", createResp)
		return errors.New("Failed to create index")
	}

	return nil
}
