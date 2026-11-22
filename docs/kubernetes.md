# Kubernetes: the cluster path for the pipeline

How the hub runs on Kubernetes (k8s) - what changed on the machine, what each
manifest means and why, the yaml syntax worth knowing, and the debugging stories
from the first live run. This is the learning doc behind issues #35/#36.

## What changed on the machine (docker desktop)

- **nothing was replaced**: docker desktop still runs every stack container exactly
  as before. The cluster was added as *one more docker container*: `kind` (kubernetes
  in docker - the official testing tool, free, apache 2.0) runs a full single-node
  kubernetes control plane inside a container named
  `banking-datahub-control-plane`.
- the `kubectl` (kubernetes cli) binary ships with docker desktop; `kind` is the
  cluster manager (`kind create cluster --name banking-datahub` / `kind delete cluster`).
- the kubectl context switched to `kind-banking-datahub` - `kubectl config get-contexts`
  shows which cluster commands hit.
- one network join makes the worlds talk: the kind node container is attached to the
  compose network (`docker network connect banking-datahub_default banking-datahub-control-plane`),
  so **pods resolve `s3` and `postgres` by name** - the same service names the compose
  path uses.
- the docker-desktop kubernetes toggle was NOT used: its enable state lives in docker
  desktop's internal config (4.86 ignored the settings-file key) and kind is the same
  kubernetes, more portable.

## The three manifests, and why each exists

### `k8s/00-namespace.yaml` - the boundary

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: banking-datahub
```

A namespace is a folder inside the cluster: everything the hub creates lands there,
names can't collide with other tenants, and deleting the folder deletes the world.
`apiVersion`/`kind` say which object schema to use; `metadata` is its identity; the
rest (here none) is the `spec` - the desired state.

### `k8s/10-spark-rbac.yaml` - the permission contract

Three objects, one contract: **spark native kubernetes mode works by the driver pod
talking to the kube api to create its executor pods** - so the job needs a service
account with exactly those rights, nothing more:

```yaml
kind: ServiceAccount   # the identity pods run as
kind: Role             # rules scoped to ONE namespace (not cluster-wide)
  rules:
    - apiGroups: [""]           # "" = the core api group (pods, services, configmaps)
      resources: ["pods"]       # what objects
      verbs: ["get", "list", "watch", "create", "delete", "patch", "update"]  # what actions
kind: RoleBinding      # attaches the Role to the ServiceAccount
  subjects:  [...]     # who gets the rights
  roleRef:   [...]     # which role
```

`verbs` are the vocabulary of the kube api. The first live run taught the exact list:
spark not only **creates** pods and **creates** its driver service - it **patches**
them (status, selectors). Missing `patch` = the driver pod fails with
`Forbidden ... cannot patch resource "services"` seconds after start. Least privilege,
but complete.

### `k8s/99-smoke-test.yaml` - the proof

A one-shot pod: `restartPolicy: Never`, busybox, echo. The k8s equivalent of
hello-world: if it reaches `Succeeded`, the cluster schedules, pulls images and runs
containers in the namespace. `kubectl logs` reads its output.

Yaml mechanics worth knowing: a file can hold several documents separated by `---`
(each becomes an object); indentation is the structure; `kubectl apply -f <file>`
declares desired state (apply again = reconcile, nothing duplicated).

## The jobs: spark-submit as a kubernetes Job

`k8s/30-spark-jobs.yaml` runs the six pipeline steps as `batch/v1 Jobs` (a Job = run
a pod until it completes successfully):

- the pod's command is `spark-submit --master k8s://https://kubernetes.default.svc
  --deploy-mode cluster` - the submitter pod authenticates to the api server with its
  own service-account token (in-cluster, no kubeconfig needed) and asks it to create
  the **driver pod**; the driver runs the app in `local[*]` mode inside its own pod
- the jars travel inside the image (`k8s/spark.Dockerfile` bakes the four fat jars;
  pods reference them as `local:///opt/spark/jobs/...` - `local://` means "already in
  the image", no upload step)
- **the environment gotcha that cost the first live run**: env vars set on the
  *submitter* container do NOT reach the *driver* pod. The driver read its default
  endpoint (`http://localhost:8333`) and spent minutes in silent s3a retries before
  surfacing `Connection refused`. Fix: pass config as
  `--conf spark.kubernetes.driverEnv.S3A_ENDPOINT=http://s3:8333` (driverEnv = the
  driver pod's environment)
- `--conf fs.s3a.attempts.maximum=1` (fast-fail) is how the silent retry loop was
  unmasked: the s3a (s3 connector) `Invoker` retries failures silently and only logs
  at debug - a hung first attempt looks exactly like a healthy, quiet pod

Debugging lessons (transferable to any k8s spark incident): `SIGQUIT` to the driver
jvm dumps all threads to the log (the hung thread was sleeping inside the s3a
`Invoker`); one job at a time (six `local[*]` apps at once thrash a single node);
and kubectl logs job/... reads only the job pod - the real output lives in the
`...-driver` pod.

## The dashboards

- **k9s** (terminal ui, the one used in production oncall): `winget install k9s`,
  then `k9s -n banking-datahub` - type `:pods`, `:jobs`, `0` to sort by age; logs are
  one key away (`l`).
- the airflow ui stays the orchestrator view (http://localhost:8080); kibana the
  data view; k9s the cluster view.

## Status

| issue | what | state |
|---|---|---|
| #35 | cluster + namespace + rbac + smoke pod | done (commit 6b490fb) |
| #36 | the six pipeline jobs as cluster pods | done (this document's commits) |
| #37 | airflow tasks as pods (KubernetesPodOperator) | open |
| #38 | kubernetes runbook section | open (this doc covers the learning half) |
