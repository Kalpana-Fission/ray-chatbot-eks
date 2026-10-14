# Ray Chatbot on EKS

A chatbot deployed on a Ray cluster running on Amazon EKS, demonstrating three layers of autoscaling: Ray Serve replicas, Kubernetes pods (via KubeRay), and EC2 nodes (via Cluster Autoscaler).

See [`docs/DOCUMENTATION.md`](docs/DOCUMENTATION.md) for the full guide (architecture, concepts, KEDA vs KubeRay, deployment steps, commands, troubleshooting).

## What's here

| File | Purpose |
|---|---|
| `chatbot_serve.py` | The Ray Serve application — chatbot logic, autoscaling config, HTTP routes |
| `raycluster.yaml` | RayCluster spec — head/worker pod resources, scaling bounds, probes |
| `Dockerfile` | Builds the chatbot image |
| `cluster-reader-rbac.yaml` | RBAC so the app can read live node/pod state |
| `.github/workflows/build-push-ecr.yml` | CI: builds and pushes the image to ECR on every push |
| `chatbot_load_test.py` | Generates real concurrent load to trigger autoscaling |
| `chat_console.py` | Interactive terminal chat client |
| `ray_live_dashboard.html` | Local live dashboard — watch replicas/pods/nodes scale in real time |

## Quick start

```bash
export AWS_PROFILE=<your-sso-profile>

# 1. Infra (see docs/DOCUMENTATION.md for the full Terraform paths)
terraform apply   # VPC, then EKS

# 2. Connect
aws eks update-kubeconfig --name <cluster-name> --region <region>

# 3. KubeRay + Cluster Autoscaler
helm install kuberay-operator kuberay/kuberay-operator --version 1.2.2
helm install cluster-autoscaler autoscaler/cluster-autoscaler --namespace kube-system ...

# 4. App
kubectl apply -f cluster-reader-rbac.yaml
kubectl create secret generic nvidia-api-key --from-literal=api-key='<your-key>'
kubectl apply -f raycluster.yaml
kubectl exec <head-pod> -c ray-head -- serve run chatbot_serve:app

# 5. Access
kubectl port-forward svc/<ray-cluster-name>-head-svc 8000:8000
```

Then open `ray_live_dashboard.html` in a browser, or run `python chat_console.py`.

## Cost note

This runs real AWS infrastructure (EKS control plane, EC2 nodes, NAT Gateway) and bills by the hour. Run `terraform destroy` (EKS module, then VPC module) when not actively using it.
