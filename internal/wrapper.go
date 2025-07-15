/*
Copyright 2022 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package internal

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"k8s.io/klog/v2"
	yaml "sigs.k8s.io/yaml/goyaml.v3"

	"k8s.io/kube-state-metrics/v2/pkg/app"
	"k8s.io/kube-state-metrics/v2/pkg/options"
)

// RunKubeStateMetricsWrapper is a wrapper around KSM, delegated to the root command.
func RunKubeStateMetricsWrapper(opts *options.Options) {

	var (
		KSMRunOrDie func(ctx context.Context)
		ksmDone     chan struct{}
	)

	KSMRunOrDie = func(ctx context.Context) {
		defer close(ksmDone)
		if err := app.RunKubeStateMetricsWrapper(ctx, opts); err != nil {
			klog.ErrorS(err, "Failed to run kube-state-metrics")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	ksmDone = make(chan struct{})

	if file := options.GetConfigFile(*opts); file != "" {
		cfgViper := viper.New()
		cfgViper.SetConfigType("yaml")
		cfgViper.SetConfigFile(file)
		if err := cfgViper.ReadInConfig(); err != nil {
			if errors.Is(err, viper.ConfigFileNotFoundError{}) {
				klog.ErrorS(err, "Options configuration file not found", "file", file)
			} else {
				klog.ErrorS(err, "Error reading options configuration file", "file", file)
			}
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		cfgViper.OnConfigChange(func(e fsnotify.Event) {
			klog.InfoS("Changes detected", "name", e.Name)
			cancel()
			<-ksmDone // 等待 KSM 完全退出
			// Wait for the ports to be released.
			<-time.After(3 * time.Second)
			ctx, cancel = context.WithCancel(context.Background())
			ksmDone = make(chan struct{})
			go KSMRunOrDie(ctx)
		})
		cfgViper.WatchConfig()

		// Merge configFile values with opts so we get the CustomResourceConfigFile from config as well
		configFile, err := os.ReadFile(filepath.Clean(file))
		if err != nil {
			klog.ErrorS(err, "failed to read options configuration file", "file", file)
		}

		yaml.Unmarshal(configFile, opts)
	}
	if opts.CustomResourceConfigFile != "" {
		crcViper := viper.New()
		crcViper.SetConfigType("yaml")
		crcViper.SetConfigFile(opts.CustomResourceConfigFile)
		if err := crcViper.ReadInConfig(); err != nil {
			if errors.Is(err, viper.ConfigFileNotFoundError{}) {
				klog.ErrorS(err, "Custom resource configuration file not found", "file", opts.CustomResourceConfigFile)
			} else {
				klog.ErrorS(err, "Error reading Custom resource configuration file", "file", opts.CustomResourceConfigFile)
			}
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		crcViper.OnConfigChange(func(e fsnotify.Event) {
			klog.InfoS("Changes detected", "name", e.Name)
			cancel()
			<-ksmDone // 等待 KSM 完全退出
			<-time.After(3 * time.Second)
			ctx, cancel = context.WithCancel(context.Background())
			ksmDone = make(chan struct{})
			go KSMRunOrDie(ctx)
		})
		crcViper.WatchConfig()
	}
	if opts.Kubeconfig != "" {
		lastMD5, _ := calculateMD5(opts.Kubeconfig)

		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					currentMD5, err := calculateMD5(opts.Kubeconfig)
					if err != nil {
						klog.ErrorS(err, "Failed to calculate MD5", "file", opts.Kubeconfig)
						continue
					}

					if currentMD5 != lastMD5 {
						klog.InfoS("File changed detected by MD5", "file", opts.Kubeconfig)
						lastMD5 = currentMD5

						cancel()
						<-ksmDone
						<-time.After(3 * time.Second)
						ctx, cancel = context.WithCancel(context.Background())
						ksmDone = make(chan struct{})
						go KSMRunOrDie(ctx)
					}

				}
			}
		}()
	}
	klog.InfoS("Starting kube-state-metrics")
	go KSMRunOrDie(ctx)
	select {}
}

func calculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
