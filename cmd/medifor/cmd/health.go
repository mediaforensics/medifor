package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
)

// healthCmd represents the health command
var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Send health check request.",
	Long:  `Send a health check request to a running service.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		client := mustClient(ctx)
		defer client.Close()

		resp, err := client.Health(ctx)
		if err != nil {
			log.Fatalf("Error getting health: %v", err)
		}
		fmt.Println(proto.MarshalTextString(resp))
	},
}

func init() {
	rootCmd.AddCommand(healthCmd)
}
