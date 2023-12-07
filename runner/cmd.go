package runner

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/theckman/yacspin"
)

var (
	batchDir string
	url      string

	selectorDir string
	backupDir   string
	backfillDir string
)

var spinnerCfg = yacspin.Config{
	Frequency:       100 * time.Millisecond,
	CharSet:         yacspin.CharSets[47],
	Suffix:          " backing up database to S3",
	SuffixAutoColon: true,
	StopCharacter:   "✓",
	StopMessage:     "DONE!",
	StopColors:      []string{"fgGreen"},
	StopFailMessage: "✘",
	StopFailColors:  []string{"fgRed"},
}

var cmd = &cobra.Command{
	Use:   "run",
	Short: "run generated sql",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		selectorDir = fmt.Sprintf("%s/selector", batchDir)
		backupDir = fmt.Sprintf("%s/backup", batchDir)
		backfillDir = fmt.Sprintf("%s/backfill", batchDir)

		// create backup folder if not exists
		_ = os.Mkdir(backupDir, 0777)

		f, err := os.ReadDir(selectorDir)
		if err != nil {
			return err
		}

		batchSize := len(f)

		conn, err := pgxpool.New(ctx, url)
		if err != nil {
			return err
		}
		defer conn.Close()

		for i := 0; i < batchSize; i++ {
			batchTxt := fmt.Sprintf("batch %d\t", i)

			s, _ := yacspin.New(spinnerCfg)
			s.Prefix(batchTxt)
			_ = s.Start()

			s.Suffix("backup data")
			if err = backup(ctx, conn, i); err != nil {
				errMsg := fmt.Errorf("backup error: %s", err.Error())
				s.StopFailMessage(errMsg.Error())
				s.StopFail()
				continue
			}

			s.Suffix("backfill data")
			if err = backfill(ctx, conn, i); err != nil {
				errMsg := fmt.Errorf("backup error: %s", err.Error())
				s.StopFailMessage(errMsg.Error())
				s.StopFail()
				continue
			}

			time.Sleep(1 * time.Second)
			_ = s.Stop()
		}

		fmt.Println("\nall backfilling done")
		// TODO: make this into args
		return nil
	},
}

func backup(ctx context.Context, conn *pgxpool.Pool, batch int) error {
	var data []map[string]interface{}
	selectB, err := os.ReadFile(fmt.Sprintf("%s/batch_%d.sql", selectorDir, batch))
	if err != nil {
		return err
	}

	rows, err := conn.Query(ctx, string(selectB))
	defer rows.Close()
	if err != nil {
		return err
	}

	data, err = pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	headers := make([]string, 0)
	for key := range data[0] {
		headers = append(headers, key)
	}

	backup, err := os.Create(fmt.Sprintf("%s/batch_%d.csv", backupDir, batch))
	defer backup.Close()
	if err != nil {
		return err
	}

	_, err = backup.Write([]byte(strings.Join(headers, ",") + "\n"))
	if err != nil {
		return err
	}

	for _, row := range data {
		line := make([]string, 0)
		for _, key := range headers {
			line = append(line, fmt.Sprintf("%v", row[key]))
		}

		_, err = backup.Write([]byte(strings.Join(line, ",") + "\n"))
		if err != nil {
			return err
		}
	}

	return nil
}

func backfill(ctx context.Context, conn *pgxpool.Pool, batch int) error {
	updateB, err := os.ReadFile(fmt.Sprintf("%s/batch_%d.sql", backfillDir, batch))
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, string(updateB))
	if err != nil {
		return err
	}

	return nil
}

func Init() *cobra.Command {
	cmd.Flags().StringVarP(&batchDir, "source", "s", "output", " directory containing generated batched sql")
	cmd.Flags().StringVar(&url, "url", "postgres://user:pass@someurl.com/db", "db url connection")

	_ = cmd.MarkFlagRequired("url")
	return cmd
}
