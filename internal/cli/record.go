package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
	"github.com/kevinmingtarja/k8s-time-travel/internal/client"
	"github.com/kevinmingtarja/k8s-time-travel/internal/server"
)

func newDaemonCmd(o *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Run the daemon in the foreground (other commands start it automatically)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return server.Run(o.home)
		},
	}
}

func newShutdownCmd(o *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "shutdown",
		Short: "Stop the daemon; running recordings resume (with a gap) when it next starts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(o.home)
			if _, err := c.Ping(); err != nil {
				fmt.Println("daemon is not running")
				return nil
			}
			if err := c.Shutdown(); err != nil {
				return err
			}
			for range 100 {
				if _, err := c.Ping(); err != nil {
					fmt.Println("daemon stopped")
					return nil
				}
				time.Sleep(50 * time.Millisecond)
			}
			return fmt.Errorf("daemon still answering after 5s")
		},
	}
}

func newRecordCmd(o *rootOptions) *cobra.Command {
	var (
		spec          api.RecordingSpec
		allNamespaces bool
		kubeconfig    string
	)
	cmd := &cobra.Command{
		Use:   "record <resource> --context <ctx> [-n ns | -A] [-l selector]",
		Short: "Start recording every change to the selected objects",
		Long: `Start recording a resource. The daemon lists the objects, then watches them,
storing every revision. Recording continues until 'ktt stop', across daemon
restarts (each restart leaves a gap).

With --events (default), core Events about objects of the same kind are
recorded too and shown interleaved in 'ktt history'.

Recording the same selection twice is a no-op that returns the existing
recording.`,
		Example: `  ktt record pods --context kind-dev -n default -l app=web
  ktt record deploy --context kind-dev -A
  ktt record nodes --context kind-dev --events=false`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if spec.Context == "" {
				return fmt.Errorf("--context is required (the kubeconfig's current context can change under you); e.g. --context $(kubectl config current-context)")
			}
			if allNamespaces && spec.Namespace != "" {
				return fmt.Errorf("-n and -A are mutually exclusive")
			}
			spec.Resource = args[0]
			switch {
			case kubeconfig != "":
				abs, err := filepath.Abs(kubeconfig)
				if err != nil {
					return err
				}
				spec.Kubeconfig = []string{abs}
			case os.Getenv("KUBECONFIG") != "":
				for _, p := range filepath.SplitList(os.Getenv("KUBECONFIG")) {
					if p == "" {
						continue
					}
					abs, err := filepath.Abs(p)
					if err != nil {
						return err
					}
					spec.Kubeconfig = append(spec.Kubeconfig, abs)
				}
			}
			c, err := o.client()
			if err != nil {
				return err
			}
			recs, err := c.Record(server.RecordRequest{Spec: spec, AllNamespaces: allNamespaces})
			for _, r := range recs {
				fmt.Printf("recording %s: %s (%d revisions so far)\n", r.ID, describeRec(r), r.Revisions)
			}
			return err
		},
	}
	f := cmd.Flags()
	f.StringVar(&spec.Context, "context", "", "kubeconfig context (required)")
	f.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig file (default: $KUBECONFIG or ~/.kube/config)")
	f.StringVarP(&spec.Namespace, "namespace", "n", "", "namespace (default: the context's namespace)")
	f.BoolVarP(&allNamespaces, "all-namespaces", "A", false, "record in all namespaces")
	f.StringVarP(&spec.LabelSelector, "selector", "l", "", "label selector")
	f.StringVar(&spec.FieldSelector, "field-selector", "", "field selector")
	f.BoolVar(&spec.Events, "events", true, "also record core Events about objects of this kind")
	return cmd
}

func describeRec(r api.Recording) string {
	s := r.Info.GVR()
	if r.Info.Namespaced {
		if r.Spec.Namespace == "" {
			s += " in all namespaces"
		} else {
			s += " in " + r.Spec.Namespace
		}
	}
	if r.Spec.LabelSelector != "" {
		s += " -l " + r.Spec.LabelSelector
	}
	if r.Spec.FieldSelector != "" {
		s += " --field-selector " + r.Spec.FieldSelector
	}
	return s + " on " + r.Spec.Context
}

func newLsCmd(o *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List recordings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := o.client()
			if err != nil {
				return err
			}
			recs, err := c.Recordings()
			if err != nil {
				return err
			}
			if len(recs) == 0 {
				fmt.Println("no recordings; start one with: ktt record <resource> --context <ctx>")
				return nil
			}
			now := time.Now()
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "ID\tSTATE\tRECORDING\tSTARTED\tLAST SYNC\tREVISIONS")
			var errs []string
			for _, r := range recs {
				id := r.ID
				if r.ParentID != "" {
					id = "└ " + id
				}
				sync := "-"
				if r.LastSync > 0 {
					sync = fmtDur(now.Sub(time.Unix(0, r.LastSync))) + " ago"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n", id, r.State, describeRec(r), fmtTS(r.StartedAt), sync, r.Revisions)
				if r.LastError != "" {
					errs = append(errs, fmt.Sprintf("%s: %s", r.ID, r.LastError))
				}
			}
			w.Flush()
			for _, e := range errs {
				fmt.Println("error " + e)
			}
			return nil
		},
	}
}

func newStopCmd(o *rootOptions) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "stop <id>... | --all",
		Short: "Stop recordings (their data stays queryable)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if all == (len(args) > 0) {
				return fmt.Errorf("pass recording ids or --all")
			}
			c, err := o.client()
			if err != nil {
				return err
			}
			if all {
				recs, err := c.Recordings()
				if err != nil {
					return err
				}
				for _, r := range recs {
					if r.State != api.StateStopped && r.ParentID == "" {
						args = append(args, r.ID)
					}
				}
			}
			var failed []string
			for _, id := range args {
				recs, err := c.Stop(id)
				if err != nil {
					failed = append(failed, fmt.Sprintf("%s: %v", id, err))
					continue
				}
				for _, r := range recs {
					fmt.Printf("stopped %s: %s\n", r.ID, describeRec(r))
				}
			}
			if len(failed) > 0 {
				return fmt.Errorf("%s", strings.Join(failed, "; "))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "stop every running recording")
	return cmd
}
