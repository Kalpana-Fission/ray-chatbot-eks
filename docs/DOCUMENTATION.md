# Ray Cluster & KubeRay on EKS — Guide

## What this project is

This project runs a chatbot on **Ray**, a framework for running Python workloads across many machines, deployed on **Kubernetes (EKS)** using **KubeRay**, the operator that lets Kubernetes manage a Ray cluster.

In one sentence: Ray handles running and scaling the AI workload; Kubernetes handles running and scaling the machines underneath it; KubeRay is the bridge that lets Ray tell Kubernetes what it needs.

Why not just plain Kubernetes? A normal Kubernetes Deployment scales identical copies of one container. Ray adds something extra: it understands Python-level work (actors, tasks, model replicas) and can pack several of them efficiently onto the same machine, not just one-container-per-pod. That matters for AI workloads where a single request might need a GPU, a large chunk of memory, or has to share state with other requests.

## Architecture

```
Browser
  |
kubectl port-forward (localhost:8000)
  |
Ray Serve Proxy (head pod)
  |
  v
+----------------------------------------------------------------+
| Layer 1 -- Ray Serve replicas                                  |
| Scaled by Ray Serve's own autoscaler, based on ongoing         |
| requests per replica. Range: 1-10 replicas.                    |
+----------------------------------------------------------------+
  |
  v
+----------------------------------------------------------------+
| Layer 2 -- Kubernetes pods                                     |
| Scaled by the KubeRay operator, reacting to Ray's internal     |
| resource requests. Range: 1-5 worker pods.                     |
+----------------------------------------------------------------+
  |
  v
+----------------------------------------------------------------+
| Layer 3 -- EC2 nodes                                           |
| Scaled by Cluster Autoscaler, reacting to Pending pods it      |
| can't place. Range: 1-6 nodes (t3.medium).                     |
+----------------------------------------------------------------+
```

A request comes in through the Ray Serve proxy running on the head pod. Below that are three independent layers, each watching the one above it and scaling the one below it:

| Layer | Who controls it | What it scales |
|---|---|---|
| 1. Ray Serve replicas | Ray Serve's own autoscaler | Number of chatbot "workers" (Python actors), 1 to 10 |
| 2. Kubernetes pods | KubeRay operator | Number of Ray worker pods, 1 to 5 |
| 3. EC2 nodes | Cluster Autoscaler | Number of EC2 machines, 1 to 6 |

None of these three know about the other two's internals. Ray Serve just knows "I need more CPU." KubeRay turns that into "create another pod." Cluster Autoscaler turns an unschedulable pod into "launch another EC2 instance." That separation is deliberate — each tool does one job well instead of one tool trying to do everything.

## Core concepts

**Ray cluster.** A group of machines running the Ray runtime together: one **head** node (coordinates everything, runs the autoscaler and dashboard) and one or more **worker** nodes (do the actual work). Here the "machines" are Kubernetes pods, not raw EC2 instances.

**Ray Serve.** Ray's library for serving a Python class as an HTTP API with autoscaling. Our chatbot is one Ray Serve deployment (`ChatBot` in `chatbot_serve.py`).

**Replica.** One running copy of that deployment, a Python object that handles requests. Ray Serve creates and removes replicas automatically based on load. Multiple replicas can share one pod if there is spare CPU.

**KubeRay operator.** A Kubernetes controller, running as its own pod, that watches for `RayCluster` objects and makes Kubernetes match what they describe: creating worker pods, deleting them, keeping the head pod alive.

**RayCluster (the YAML).** `raycluster.yaml` in this repo is the spec KubeRay reads: how much CPU/memory the head and workers get, how many workers are allowed (`minReplicas` / `maxReplicas`), and which Docker image to run.

### How the Ray cluster works here

1. We apply `raycluster.yaml`. KubeRay creates one head pod and one worker pod.
2. We run `serve run chatbot_serve:app` on the head pod, which deploys the `ChatBot` class as a Ray Serve app.
3. Ray Serve's own autoscaler (set in `chatbot_serve.py`, not the YAML) watches request load and adjusts replica count, 1 to 10.
4. If replicas need more CPU than existing pods have, Ray's internal autoscaler asks for another worker pod.
5. That request changes the RayCluster object's desired worker count. KubeRay notices and creates the pod.
6. If no EC2 node has room for that pod, it sits `Pending` until Cluster Autoscaler adds a node (a separate tool, not part of Ray or KubeRay at all).

### How KubeRay works here

KubeRay does not know anything about chatbots, replicas, or HTTP traffic. It only reads one thing: how many worker pods the `RayCluster` object currently asks for, and makes Kubernetes match that number. The actual decision of "we need more workers" is made inside Ray itself (step 4 above) and expressed as a change to that same object. KubeRay is the translator between "Ray wants more capacity" and "Kubernetes, please create a pod."

### Ray dashboard

Every Ray cluster runs a built-in web dashboard on the head pod, port 8265. It is separate from our chatbot's own `/metrics` endpoint and shows Ray's own view of the cluster:

- **Jobs** tab: every script run against the cluster (like our load test), its status, and logs.
- **Actors** tab: every live Python actor, including each Serve replica, with its CPU/memory and which node it is on.
- **Cluster** tab: every node in the Ray cluster (head + workers), their resource usage, and whether the autoscaler is adding or removing any.
- **Serve** tab: your deployed apps (`ChatBot`), their replica count, and per-replica request stats, updated live.
- **Metrics** tab: Prometheus-style graphs if a Prometheus/Grafana stack is wired in (not set up in this project, so this tab is mostly empty here).

To view it: `kubectl port-forward svc/ray-sentiment-cluster-head-svc 8265:8265`, then open `http://localhost:8265`. It is the fastest way to see replica scaling live without writing any code, and is a good first stop when something looks wrong — it shows actor failures and restarts that plain `kubectl` logs do not surface as clearly.

## KEDA vs KubeRay

**KEDA** (Kubernetes Event-Driven Autoscaling) is a general-purpose autoscaler for any Kubernetes workload. It watches an external signal (queue depth, HTTP request rate, a Prometheus metric, a cron schedule) and translates it into a standard `HorizontalPodAutoscaler`, scaling a plain Deployment's pod count up or down.

**KubeRay** is Ray-specific. It does not watch external metrics at all. It watches one thing: what the `RayCluster` object says it needs, and that number comes from Ray's own internal autoscaler, which understands Ray concepts KEDA has no concept of: actors, tasks, Serve replicas, and how to pack several of them onto one pod.

| | KEDA | KubeRay |
|---|---|---|
| Scope | Any Kubernetes workload | Only Ray (`RayCluster`, `RayJob`, `RayService`) |
| Decides scaling from | External metrics you wire up (queue length, HTTP rate, custom Prometheus query) | Ray's own internal autoscaler, which tracks actor and task placement across the whole Ray cluster |
| Granularity | Pod-level only | Actor/task-level — several replicas can share one pod |
| What it changes | A Deployment's replica count via HPA | A `RayCluster`'s worker pod count |

### Why we need Ray and KubeRay when KEDA exists

KEDA could scale a plain web server based on request rate, but it has no idea what is happening *inside* a pod. It cannot say "this pod has room for one more 0.5-CPU replica" or "these five replicas should share two pods instead of five" — to KEDA, a pod is a black box; it only counts pods.

Our chatbot's scaling decision (`autoscaling_config` in `chatbot_serve.py`) is built on `target_ongoing_requests`, a Ray Serve-native idea: how many requests is each replica juggling right now. That number lives inside the Ray process, not in an external metrics store KEDA could query. Reproducing this with KEDA would mean exporting Ray's internal state to Prometheus first, then having KEDA read it back and drive a generic HPA — extra moving parts to approximate something KubeRay already does natively, with less precision (KEDA still could not pack multiple replicas per pod).

In short: KEDA is the right tool when you are scaling a stateless web app based on a metric you can observe from outside. KubeRay is the right tool when the thing scaling is Ray itself, and the scaling decision needs to understand what is happening inside the Ray processes, not just how many pods exist.

## Why Ray instead of plain Kubernetes + HPA

This is the most important practical reason this project uses Ray, so it is worth spelling out with real numbers from this project.

**This chatbot's workload is I/O-bound, not CPU-bound.** Each request spends almost all its time waiting on a network call to an external model API (NVIDIA's), not computing anything locally. In our own Cluster Autoscaler logs, nodes actively serving chatbot traffic showed CPU utilization around **7-15%** — low, even under real load.

**A standard CPU-based HPA would never scale this app.** It watches CPU%, and CPU% barely moves no matter how many concurrent requests are queued, because the bottleneck is network wait, not computation. A deployment behind a CPU-based HPA would sit at its minimum replica count forever while requests piled up.

**Ray Serve's autoscaler watches `ongoing requests per replica` instead** — a number that directly reflects real demand, independent of CPU. This is the actual reason the chatbot could scale correctly under load in this project.

### Replicas vs requests — what `max_replicas: 10` actually means

`max_replicas: 10` is a ceiling on **worker instances**, not a ceiling on **concurrent requests**, and it's worth being precise about what the numbers around it do and don't guarantee:

- `max_replicas: 10` — the max number of Python actor instances
- `max_ongoing_requests` (Ray Serve default: `5`) — how many requests the proxy will **assign** to a replica before it's gotten a response back for any of them. It's an admission limit, not a promise that all 5 run in parallel at that instant.

With 10 replicas at that default, the proxy will not have more than `10 × 5 = 50` requests assigned-and-unanswered across the deployment at once — that's the real ceiling, phrased carefully. Whether those 50 are genuinely progressing simultaneously depends on whether each replica's code actually yields the event loop instead of blocking it. In this project that caveat was not theoretical: the first version made a blocking `requests.post()` call inside an `async def` handler, which silently serialized every request onto a single replica no matter how many existed. The fix — `asyncio.to_thread(...)` around the blocking call — is what makes the 50-admission ceiling actually mean something (see "Lessons learned" below).

`target_ongoing_requests: 2` is the **trigger**, not the hard cap: "if a replica averages more than ~2 requests in flight, add another replica." The autoscaler prefers spreading load thin across many replicas rather than stacking requests onto a few.

**What happens once you hit `max_replicas`.** It's a hard ceiling — the Ray autoscaler will not ask KubeRay for an 11th replica no matter how much demand there is, which means no further pod or node scaling happens either, since nothing new was requested. Extra load beyond that point doesn't get rejected; it waits in the proxy's backlog. Each of the 10 replicas keeps handling requests up to its own `max_ongoing_requests` limit, and the moment one finishes and frees a slot, the proxy hands it the next waiting request. So `max_replicas` caps **spend**, not what eventually gets served — past the ceiling you're trading latency for cost, not losing requests.

### Ray does not make any single request faster

Ray does not speed up the network call to the model API — that takes exactly as long either way. What Ray changes is **how many requests can be in flight at once**, which determines how fast a *batch* of requests finishes, not the latency of any individual one. 500 requests processed with real concurrency finish in a couple of minutes instead of tens of minutes, because dozens are being handled simultaneously instead of one at a time — not because each one got quicker.

### The honest tradeoff: Ray is not free

Each pod in this project reserves 2.5-3 GiB of memory just for the Ray runtime itself (GCS, dashboard processes on the head; raylet overhead on workers) — far more than a bare FastAPI container would need (a few hundred MiB). Per node, you could fit more lightweight plain pods than Ray pods if all you needed was dumb horizontal scaling. Ray's overhead buys request-aware scaling and the ability to pack multiple replicas into one pod — but that intelligence has a real memory cost.

## Where scaling thresholds are actually defined

Three different layers, three different places, none of them a simple "CPU %" everywhere:

| Layer | Threshold concept | Where configured |
|---|---|---|
| Replicas | Requests per replica (not CPU) | `chatbot_serve.py` → `autoscaling_config.target_ongoing_requests` |
| Pods | Implicit — pure CPU math, no percentage | `chatbot_serve.py` `ray_actor_options.num_cpus` + `raycluster.yaml` `resources` blocks |
| Node scale-**up** | None — purely reacts to `Pending` pods | N/A, always reactive, no threshold at all |
| Node scale-**down** | CPU/memory utilization % | Cluster Autoscaler flag `--scale-down-utilization-threshold` (default 50%, not explicitly set in this project) |

Node-level scaling bounds (how many EC2 nodes are allowed to exist) are defined separately again, in Terraform, not in any Ray/Kubernetes YAML:

```hcl
# 07_Terraform_EKS_Cluster/02_EKS_terraform-manifests/c9_eks_nodegroup_private.tf
scaling_config {
  desired_size = 3
  min_size     = 1
  max_size     = 6
}
```

And the actual CPU/memory *per node* comes from the instance type, set in `terraform.tfvars`:

```hcl
node_instance_types = ["t3.medium"]  # 2 vCPU, 4 GiB per node — AWS's fixed spec for this type
```

## RayService — what it is and why we don't use one

KubeRay provides three custom resources: `RayCluster` (raw compute), `RayJob` (one-shot batch work), and `RayService` (a production-oriented wrapper specifically for Ray Serve apps). A `RayService` manifest bundles both the cluster spec and the Serve app config into one declarative object.

This project uses the manual two-step approach instead:
1. `kubectl apply -f raycluster.yaml` — creates compute
2. `kubectl exec ... serve run chatbot_serve:app` — deploys the app by hand

That was the right call for a hands-on learning project — every step stays separately visible and debuggable, which mattered while diagnosing the OOM and probe issues documented below.

| Feature | Manual (what we have) | `RayService` (what we don't have) |
|---|---|---|
| App deployment | Must run `serve run` by hand after every pod restart | KubeRay deploys the Serve app automatically |
| Survives pod/cluster recreation | No — re-run `serve run` yourself | Yes — KubeRay re-applies it automatically |
| Zero-downtime upgrades | No — deleting pods briefly drops traffic | Yes — new cluster spun up, traffic cut over once healthy |
| Self-healing | No | Yes — replaces an unhealthy RayCluster automatically |

For a production deployment of this chatbot, converting `raycluster.yaml` into a `rayservice.yaml` would be the natural next step.

## Files in this repo

| File | What it does |
|---|---|
| `chatbot_serve.py` | The Ray Serve application. Defines the `ChatBot` class, its autoscaling config, the `/`, `/chat`, `/metrics`, `/nodes` HTTP routes, and the call to the NVIDIA model API. |
| `raycluster.yaml` | The RayCluster spec: head and worker pod resources, image, worker scaling bounds, probes, environment variables (API keys from Secrets). |
| `Dockerfile` | Builds the chatbot image: starts from `rayproject/ray`, installs `requests`, `fastapi`, `pydantic`, `kubernetes`, copies in `chatbot_serve.py`. |
| `cluster-reader-rbac.yaml` | Grants the Ray pods' service account permission to list Kubernetes nodes and pods, used by the `/nodes` endpoint. |
| `.github/workflows/build-push-ecr.yml` | GitHub Actions workflow: builds the Docker image and pushes it to ECR on every push to `Dockerfile` or `chatbot_serve.py`. |
| `chatbot_load_test.py` | A script that fires concurrent requests at the chatbot to generate real load, used to trigger and observe autoscaling. |
| `chat_console.py` | A simple interactive terminal chat client against the deployed chatbot. |
| `ray_live_dashboard.html` | A local HTML page (open directly in a browser) that polls `/metrics` and `/nodes` and shows replica, pod, and node counts live, with a built-in load test button. |

## How this gets deployed, end to end

1. **Push code.** A change to `chatbot_serve.py` or `Dockerfile` is pushed to GitHub.
2. **GitHub Actions builds and pushes.** The workflow builds the Docker image and pushes it to Amazon ECR, tagged `latest` plus the commit SHA.
3. **Apply the RayCluster.** `kubectl apply -f raycluster.yaml` tells KubeRay what the cluster should look like (this step only needs repeating when `raycluster.yaml` itself changes).
4. **Pick up the new image.** Since pods use `imagePullPolicy: Always` with the `latest` tag, deleting the running pods (`kubectl delete pod -l ray.io/cluster=ray-sentiment-cluster`) forces them to restart on the freshly built image.
5. **Deploy the Ray Serve app.** Once the head pod is `Running`, `serve run chatbot_serve:app` (run via `kubectl exec`) deploys the actual `ChatBot` application onto the Ray cluster.
6. **Traffic arrives.** `kubectl port-forward` exposes the head pod's port 8000 locally, or (for a permanent URL) a Kubernetes `LoadBalancer` Service could front it instead.

The RayCluster (step 3) and the application deployed on it (step 5) are separate concepts: the RayCluster is just compute capacity sitting idle until something is deployed onto it with `serve run`.

## Infrastructure setup (Terraform)

This project's infrastructure lives in `devops-real-world-project-implementation-on-aws/07_Terraform_EKS_Cluster/`, as two Terraform modules applied in order:

```bash
export AWS_PROFILE=<your-sso-profile>

# 1. VPC (subnets, NAT gateway, route tables)
cd 07_Terraform_EKS_Cluster/01_VPC_terraform-manifests
terraform init && terraform apply

# 2. EKS cluster + managed node group (reads VPC outputs via remote state)
cd ../02_EKS_terraform-manifests
terraform init && terraform apply
```

### Cluster Autoscaler — the one manual step after every fresh EKS cluster

Cluster Autoscaler's IAM role trust policy is tied to the EKS cluster's **OIDC provider ARN**, which includes a cluster-specific ID. Every time the EKS cluster is destroyed and recreated, that ID changes, so this trust relationship goes stale and must be redone — reinstalling the Helm chart alone is not enough.

```bash
# Register a new OIDC provider for the new cluster
OIDC_URL=$(aws eks describe-cluster --name <cluster-name> --region <region> --query "cluster.identity.oidc.issuer" --output text | sed 's|https://||')
THUMBPRINT=$(echo | openssl s_client -servername oidc.eks.<region>.amazonaws.com -connect oidc.eks.<region>.amazonaws.com:443 2>/dev/null | openssl x509 -fingerprint -sha1 -noout | sed 's/.*=//;s/://g' | tr 'A-F' 'a-f')
aws iam create-open-id-connect-provider --url "https://$OIDC_URL" --client-id-list sts.amazonaws.com --thumbprint-list "$THUMBPRINT"

# Update the existing cluster-autoscaler IAM role's trust policy to point at the new OIDC provider
aws iam update-assume-role-policy --role-name eks-cluster-autoscaler --policy-document file://cluster-autoscaler-trust-policy.json

# Tag the new node group's ASG so Cluster Autoscaler can discover it
ASG_NAME=$(aws eks describe-nodegroup --cluster-name <cluster-name> --nodegroup-name <nodegroup-name> --region <region> --query "nodegroup.resources.autoScalingGroups[0].name" --output text)
aws autoscaling create-or-update-tags --tags \
  "ResourceId=$ASG_NAME,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/enabled,Value=true,PropagateAtLaunch=true" \
  "ResourceId=$ASG_NAME,ResourceType=auto-scaling-group,Key=k8s.io/cluster-autoscaler/<cluster-name>,Value=owned,PropagateAtLaunch=true"

# If your VPC has only 2 AZs, suspend AZRebalance — it fights odd desired-capacity values
aws autoscaling suspend-processes --auto-scaling-group-name "$ASG_NAME" --scaling-processes AZRebalance
```

### Install KubeRay and Cluster Autoscaler

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm repo update

helm install kuberay-operator kuberay/kuberay-operator --version 1.2.2

helm install cluster-autoscaler autoscaler/cluster-autoscaler \
  --namespace kube-system \
  --set autoDiscovery.clusterName=<cluster-name> \
  --set awsRegion=<region> \
  --set rbac.serviceAccount.create=true \
  --set rbac.serviceAccount.name=cluster-autoscaler \
  --set "rbac.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::<account-id>:role/eks-cluster-autoscaler" \
  --set extraArgs.balance-similar-node-groups=true \
  --set extraArgs.skip-nodes-with-system-pods=false \
  --set extraArgs.scale-down-delay-after-add=5m \
  --set extraArgs.scale-down-unneeded-time=5m
```

The last two flags shorten Cluster Autoscaler's default 10-minute cooldowns to 5 minutes — useful for a demo/learning environment where you want to see scale-down happen without a long wait; leave them at the default (or longer) for a real production cluster to avoid flapping.

## Day-to-day commands

```bash
# Check pods and nodes
kubectl get pods
kubectl get nodes

# Watch pods change live
kubectl get pods -w

# Connect to the chatbot locally
kubectl port-forward svc/ray-sentiment-cluster-head-svc 8000:8000

# Open the Ray dashboard locally
kubectl port-forward svc/ray-sentiment-cluster-head-svc 8265:8265

# Run a load test from inside the cluster (bypasses the browser's ~6-connection-per-origin cap)
kubectl cp chatbot_load_test.py <head-pod>:/home/ray/chatbot_load_test.py -c ray-head
kubectl exec <head-pod> -c ray-head -- python chatbot_load_test.py

# Check Ray's own view of replica/task health
kubectl exec <head-pod> -c ray-head -- ray status

# Read logs from the Serve controller and replicas
kubectl logs <head-pod> -c ray-head

# Check why a node group's desired capacity isn't changing
aws eks describe-nodegroup --cluster-name <cluster-name> --nodegroup-name <nodegroup-name> --query "nodegroup.scalingConfig"
```

## Troubleshooting

- **Chatbot errors under load, not crashing**: check the deployment's timeout to the model API and that blocking calls use `asyncio.to_thread`. A synchronous call inside an `async def` silently serializes every request onto one replica — this happened in this project and looked like "only 1 replica ever does real work" until fixed.
- **Pods crash with `Error` / `ExitCode: 1` right after new ones join**: check `kubectl describe pod <name>` for `OutOfMemoryError` in the events. This usually means the head or worker pod's memory limit is too small for the number of replicas it is hosting — happened in this project on both the head pod (crashed the ServeController repeatedly) and later on worker pods under heavy concurrent load. Fixed by raising memory limits and loosening the default liveness/readiness probe thresholds (longer `initialDelaySeconds`, higher `failureThreshold`).
- **Pods go `Error` right as load ends**: usually benign — KubeRay deleting a worker pod as part of a normal scale-down shows up as a non-zero exit code even though nothing crashed. Confirm by checking `kubectl get events` for a `DeletedWorkerPod` line from `raycluster/<name>`, not a kubelet-initiated kill.
- **Pods stuck `Pending`**: run `kubectl describe pod <name>` and look for `Insufficient cpu` / `Insufficient memory` in the events — this is the exact signal Cluster Autoscaler watches for. A new node should follow within a couple of minutes if the node group's `max_size` has not been reached.
- **New EC2 nodes never appear even though pods are `Pending`**: check `kubectl logs -n kube-system -l app.kubernetes.io/name=aws-cluster-autoscaler` for errors, confirm the ASG is tagged correctly, and confirm the node group's `max_size` in Terraform has not already been hit.
- **Node count flaps between two values**: if your VPC has only 2 availability zones, the ASG's `AZRebalance` process can repeatedly launch-then-terminate an instance trying to balance across AZs, fighting any desired-capacity change you make. Suspend it (see the Terraform setup section above).
- **Load testing from a browser caps out at a low replica count**: browsers limit concurrent connections to one origin to about 6, regardless of a "concurrency" number typed into any UI. Run `chatbot_load_test.py` via `kubectl exec` instead — it uses Python's `requests` library with an explicit connection pool sized to match the intended concurrency, with no such cap.
- **SSO session expired mid-task (`Token has expired`)**: re-run `aws sso login --sso-session <session-name>` and approve in the browser; Terraform state locks left behind by an interrupted command can be cleared with `terraform force-unlock -force <lock-id>` (the lock ID is printed in the error).

## Glossary

| Term | Meaning |
|---|---|
| Ray cluster | A head node plus worker nodes running the Ray runtime together |
| Ray Serve | Ray's library for deploying a Python class as a scalable HTTP API |
| Replica | One running instance of a Ray Serve deployment |
| KubeRay | The Kubernetes operator that manages `RayCluster` objects |
| RayCluster | The YAML spec describing a Ray cluster's shape, read by KubeRay |
| RayService | A KubeRay resource that bundles a RayCluster and its Serve app together, with self-healing and zero-downtime upgrades (not used in this project) |
| Cluster Autoscaler | A separate tool that adds or removes EC2 nodes based on pod scheduling pressure |
| KEDA | A general-purpose Kubernetes autoscaler driven by external metrics, not used in this project |
| Pod | A Kubernetes unit running one or more containers; here, a head or worker |
| Node | An EC2 instance that Kubernetes schedules pods onto |
| ASG | Auto Scaling Group, the AWS resource Cluster Autoscaler resizes |
| ECR | Elastic Container Registry, where the Docker image is stored |
| IRSA | IAM Roles for Service Accounts, how a pod gets AWS permissions |
| Pending (pod status) | The pod exists but no node has room for it yet |

## Cost

This project runs real billable AWS infrastructure:

- EKS control plane: ~$0.10/hour, flat, regardless of usage
- EC2 nodes (t3.medium): ~$0.0416/hour each, 1-6 running depending on load
- NAT Gateway: ~$0.045/hour

A typical idle baseline (EKS + 2 nodes + NAT) runs roughly **$0.27/hour (~$6.50/day)** while left running. Destroy the infrastructure when not actively using it:

```bash
export AWS_PROFILE=<your-sso-profile>
cd 07_Terraform_EKS_Cluster/02_EKS_terraform-manifests && terraform destroy
cd ../01_VPC_terraform-manifests && terraform destroy
```

ECR image storage, the GitHub repo, the Terraform state S3 bucket, and the IAM roles are all either free or negligible cost, and are safe to leave in place between sessions — only the EKS cluster, EC2 nodes, and NAT Gateway need to be torn down to stop billing.
