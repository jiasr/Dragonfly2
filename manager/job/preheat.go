/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package job

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	referencedocker "github.com/containerd/containerd/reference/docker"
	"github.com/containerd/containerd/remotes"
	remotesdocker "github.com/containerd/containerd/remotes/docker"
	"github.com/moby/buildkit/util/contentutil"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/net/httputils"
)

var tracer = otel.Tracer("manager")

type PreheatType string

const (
	PreheatImageType PreheatType = "image"
	PreheatFileType  PreheatType = "file"
)

const (
	timeout = 1 * time.Minute
)

var accessURLPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

var defaultHTTPClient = &http.Client{
	Timeout: timeout,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
}

type Preheat interface {
	CreatePreheat(context.Context, []model.Scheduler, types.PreheatArgs) (*internaljob.GroupJobState, error)
}

type preheat struct {
	job    *internaljob.Job
	bizTag string
}

func newPreheat(job *internaljob.Job, bizTag string) (Preheat, error) {
	return &preheat{
		job:    job,
		bizTag: bizTag,
	}, nil
}

func (p *preheat) CreatePreheat(ctx context.Context, schedulers []model.Scheduler, args types.PreheatArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPreheat, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributePreheatType.String(args.Type))
	span.SetAttributes(config.AttributePreheatURL.String(args.URL))
	span.SetAttributes(config.AttributePreheatURL.String(args.Image))
	defer span.End()

	// Initialize scheduler queues
	queues := getSchedulerQueues(schedulers)

	// Generate download files
	var files []*internaljob.PreheatRequest
	switch PreheatType(args.Type) {
	case PreheatImageType:
		manifest, err := p.getManifest(ctx, args)
		if err != nil {
			return nil, err
		}

		for _, layer := range manifest.Layers {
			ref, err := getRef(args)
			if err != nil {
				return nil, err
			}

			host, err := remotesdocker.DefaultHost(referencedocker.Domain(ref))
			if err != nil {
				return nil, err
			}

			blobURL := fmt.Sprintf("http://%s/v2/%s/blobs/%s", host, referencedocker.Path(ref), layer.Digest.String())
			req, err := http.NewRequestWithContext(ctx, "GET", blobURL, nil)
			if err != nil {
				return nil, err
			}

			auth := getAuthorizer(args)
			if err := auth.Authorize(ctx, req); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("testing")

		// files, err = p.getLayers(ctx, manifest, args)
		// if err != nil {
		// return nil, err
		// }
	case PreheatFileType:
		files = []*internaljob.PreheatRequest{
			{
				URL:     args.URL,
				Tag:     p.bizTag,
				Headers: args.Headers,
			},
		}
	default:
		return nil, errors.New("unknow preheat type")
	}

	for _, f := range files {
		logger.Infof("preheat %s %s file url: %v queues: %v", args.URL, args.Image, f.URL, queues)
	}

	return p.createGroupJob(ctx, files, queues)
}

func (p *preheat) createGroupJob(ctx context.Context, files []*internaljob.PreheatRequest, queues []internaljob.Queue) (*internaljob.GroupJobState, error) {
	signatures := []*machineryv1tasks.Signature{}
	var urls []string
	for i := range files {
		urls = append(urls, files[i].URL)
	}
	for _, queue := range queues {
		for _, file := range files {
			args, err := internaljob.MarshalRequest(file)
			if err != nil {
				logger.Errorf("preheat marshal request: %v, error: %v", file, err)
				continue
			}

			signatures = append(signatures, &machineryv1tasks.Signature{
				Name:       internaljob.PreheatJob,
				RoutingKey: queue.String(),
				Args:       args,
			})
		}
	}
	group, err := machineryv1tasks.NewGroup(signatures...)

	if err != nil {
		return nil, err
	}

	if _, err := p.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Error("create preheat group job failed", err)
		return nil, err
	}

	logger.Infof("create preheat group job successfully, group uuid: %s， urls: %s", group.GroupUUID, urls)
	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}

func (p *preheat) getManifest(ctx context.Context, args types.PreheatArgs) (ocispec.Manifest, error) {
	resolver := getResolver(args)
	ref, err := getRef(args)
	if err != nil {
		return ocispec.Manifest{}, nil
	}

	name, desc, err := resolver.Resolve(ctx, ref.String())
	if err != nil {
		return ocispec.Manifest{}, err
	}

	fetcher, err := resolver.Fetcher(ctx, name)
	if err != nil {
		return ocispec.Manifest{}, err
	}

	rc, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to fetch %s: %w", desc.Digest, err)
	}
	defer rc.Close()

	return images.Manifest(ctx, contentutil.FromFetcher(fetcher), desc, platforms.Default())
}

// getResolver returns a docker resolver.
func getResolver(args types.PreheatArgs) remotes.Resolver {
	return remotesdocker.NewResolver(remotesdocker.ResolverOptions{
		Hosts: remotesdocker.ConfigureDefaultRegistries(
			remotesdocker.WithClient(defaultHTTPClient),
			remotesdocker.WithAuthorizer(getAuthorizer(args)),
		),
		Headers: httputils.MapToHeader(args.Headers),
	})
}

// getAuthorizer returns docker.Authorizer
func getAuthorizer(args types.PreheatArgs) remotesdocker.Authorizer {
	creds := func(string) (string, string, error) {
		return args.Username, args.Password, nil
	}

	return remotesdocker.NewDockerAuthorizer(
		remotesdocker.WithAuthClient(defaultHTTPClient),
		remotesdocker.WithAuthCreds(creds),
		remotesdocker.WithAuthHeader(httputils.MapToHeader(args.Headers)),
	)
}

// getRef gets image reference named
func getRef(args types.PreheatArgs) (referencedocker.Named, error) {
	if args.Image != "" {
		return referencedocker.ParseNormalizedNamed(args.Image)
	}

	// Covert manifest url to image
	if args.URL != "" {
		r := accessURLPattern.FindStringSubmatch(args.URL)
		if len(r) != 5 {
			return nil, errors.New("parse access url failed")
		}

		return referencedocker.ParseNormalizedNamed(fmt.Sprintf("%s/%s:%s", r[2], r[3], r[4]))
	}

	return nil, errors.New("scheduler requires parameter image")
}

// getSchedulerQueues gets scheduler job queues
func getSchedulerQueues(schedulers []model.Scheduler) []internaljob.Queue {
	var queues []internaljob.Queue
	for _, scheduler := range schedulers {
		queue, err := internaljob.GetSchedulerQueue(scheduler.SchedulerClusterID, scheduler.HostName)
		if err != nil {
			continue
		}

		queues = append(queues, queue)
	}

	return queues
}

// func references(om ocispec.Manifest) []ocispec.Descriptor {
// references := make([]ocispec.Descriptor, 0, 1+len(om.Layers))
// references = append(references, om.Config)
// references = append(references, om.Layers...)
// return references
// }

// func (p *preheat) getLayers(ctx context.Context, manifest ocispec.Manifest, args types.PreheatArgs) ([]*internaljob.PreheatRequest, error) {
// ctx, span := tracer.Start(ctx, config.SpanGetLayers, trace.WithSpanKind(trace.SpanKindProducer))
// defer span.End()

// layers, err := p.parseLayers(manifest)
// if err != nil {
// return nil, err
// }

// return layers, nil
// }

// func (p *preheat) parseLayers(manifest ocispec.Manifest, args types.PreheatArgs) ([]*internaljob.PreheatRequest, error) {
// var layers []*internaljob.PreheatRequest
// layerHeader := httputils.MapToHeader(args.Headers).Clone()
// for _, layer := range manifest.Layers {
// digest := layer.Digest.String()
// layer := &internaljob.PreheatRequest{
// URL:     layerURL(image, layer.Digest.String()),
// Tag:     p.bizTag,
// Digest:  digest,
// Headers: httputils.HeaderToMap(layerHeader),
// }

// layers = append(layers, layer)
// }

// return layers, nil
// }

// func layerURL(protocol string, domain string, name string, digest string) string {
// return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", protocol, domain, name, digest)
// }

// func getAuthToken(ctx context.Context, header http.Header, preheatArgs types.PreheatArgs) (string, error) {
// ctx, span := tracer.Start(ctx, config.SpanAuthWithRegistry, trace.WithSpanKind(trace.SpanKindProducer))
// defer span.End()

// authURL := authURL(header.Values("WWW-Authenticate"))
// if len(authURL) == 0 {
// return "", errors.New("authURL is empty")
// }

// req, err := http.NewRequestWithContext(ctx, "GET", authURL, nil)
// if err != nil {
// return "", err
// }
// if preheatArgs.Username != "" && preheatArgs.Password != "" {
// req.SetBasicAuth(preheatArgs.Username, preheatArgs.Password)
// }

// resp, err := defaultHTTPClient.Do(req)
// if err != nil {
// return "", err
// }
// defer resp.Body.Close()

// body, _ := io.ReadAll(resp.Body)
// var result map[string]interface{}
// if err := json.Unmarshal(body, &result); err != nil {
// return "", err
// }

// if result["token"] == nil {
// return "", errors.New("token is empty")
// }

// token := fmt.Sprintf("%v", result["token"])
// return token, nil

// }

// func authURL(wwwAuth []string) string {
// if len(wwwAuth) == 0 {
// return ""
// }
// polished := make([]string, 0)
// for _, it := range wwwAuth {
// polished = append(polished, strings.ReplaceAll(it, "\"", ""))
// }
// fileds := strings.Split(polished[0], ",")
// host := strings.Split(fileds[0], "=")[1]
// query := strings.Join(fileds[1:], "&")
// return fmt.Sprintf("%s?%s", host, query)
// }
