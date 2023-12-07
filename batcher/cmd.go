package batcher

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	batchSize int
	output    string
	source    string
	tableName string
)

var cmd = &cobra.Command{
	Use:   "gen",
	Short: "generate batched sql files from a csv",
	RunE: func(cmd *cobra.Command, args []string) error {
		input, err := os.Open(source)
		if err != nil {
			return err
		}

		csv := csv.NewReader(input)
		raw, err := csv.ReadAll()
		if err != nil {
			return err
		}

		fmt.Print("batching...")

		headers := raw[0]
		data := make([]map[string]string, 0)
		for _, row := range raw[1:] {
			entry := make(map[string]string)
			for i := range row {
				entry[headers[i]] = row[i]
			}

			data = append(data, entry)
		}

		backfill := fmt.Sprintf("%s/%s", output, "backfill")
		selector := fmt.Sprintf("%s/%s", output, "selector")

		if err = os.RemoveAll(output); err != nil {
			return err
		}

		err = os.Mkdir(output, 0777)
		if err != nil {
			return err
		}

		err = os.Mkdir(backfill, 0777)
		if err != nil {
			return err
		}

		err = os.Mkdir(selector, 0777)
		if err != nil {
			return err
		}

		batchCount := len(data) / batchSize
		if len(data)%batchSize > 0 {
			batchCount++
		}

		for i := 0; i < batchCount; i++ {
			updateQuery := []string{"BEGIN;"}
			ids := []string{}
			for j := 0; j < batchSize; j++ {
				idx := (i * batchSize) + j

				if idx >= len(data) {
					break
				}

				row := data[idx]

				setter := make([]string, 0)
				for key, value := range row {
					if key == "id" {
						continue
					}

					setter = append(setter, fmt.Sprintf("%s = %s", key, value))
				}

				update := fmt.Sprintf("UPDATE \"%s\" SET %s WHERE id = %s;",
					tableName,
					strings.Join(setter, ", "),
					row["id"],
				)

				ids = append(ids, row["id"])
				updateQuery = append(updateQuery, update)
			}
			updateQuery = append(updateQuery, "COMMIT;")

			batchFile, err := os.Create(fmt.Sprintf("%s/batch_%d.sql", backfill, i))
			if err != nil {
				return err
			}
			_, _ = batchFile.Write([]byte(strings.Join(updateQuery, "\n")))
			if err = batchFile.Close(); err != nil {
				return err
			}

			where := []string{}
			for _, id := range ids {
				where = append(where, fmt.Sprintf("id = %s", id))
			}
			selectQuery := fmt.Sprintf("SELECT * FROM \"%s\" WHERE %s;", tableName, strings.Join(where, " OR "))
			selectFile, err := os.Create(fmt.Sprintf("%s/batch_%d.sql", selector, i))
			if err != nil {
				return err
			}
			_, _ = selectFile.Write([]byte(selectQuery))
			if err = selectFile.Close(); err != nil {
				return err
			}

		}

		fmt.Println("done")
		return input.Close()
	},
}

func Init() *cobra.Command {
	cmd.Flags().StringVarP(&output, "output", "o", "output", "output directory of the sql")
	cmd.Flags().IntVar(&batchSize, "size", 100, "number of lines of each batch file")
	cmd.Flags().StringVarP(&source, "source", "s", "./backfill.csv", "csv file to generate batch sql")
	cmd.Flags().StringVarP(&tableName, "table", "t", "orders", "table name to be backfilled")
	_ = cmd.MarkFlagRequired("source")
	_ = cmd.MarkFlagRequired("table")

	return cmd
}
