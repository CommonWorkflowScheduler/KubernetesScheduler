# Common Workflow Scheduler for Kubernetes
[![DOI](https://zenodo.org/badge/596122315.svg)](https://zenodo.org/badge/latestdoi/596122315)

In this repository, you will find the Common Workflow Scheduler for Kubernetes proposed in the paper "**How Workflow Engines Should Talk to Resource Managers: A Proposal for a Common Workflow Scheduling Interface**."

---
#### Build
```
docker build -t cws .
docker tag cws <your docker account>/cws:<version>
docker push <your docker account>/cws:<version>
```

#### API Endpoints

| #  | Resource                            | Method |
| -- | :---------------------------------- | :----: |
| 1  | /{version}/{execution}              | POST   |
| 2  | /{version}/{execution}              | DELETE |
| 3  | /{version}/{execution}/DAG/vertices | POST   |
| 4  | /{version}/{execution}/DAG/vertices | DELETE |
| 5  | /{version}/{execution}/DAG/edges    | POST   |
| 6  | /{version}/{execution}/DAG/edges    | DELETE |
| 7  | /{version}/{execution}/startBatch   | PUT    |
| 8  | /{version}/{execution}/endBatch     | PUT    |
| 9  | /{version}/{execution}/task/{id}    | POST   |
| 10 | /{version}/{execution}/task/{id}    | GET    |
| 11 | /{version}/{execution}/task/{id}    | DELETE |

SWAGGER:  /swagger-ui.html <br>
API-DOCS: /v3/api-docs

For more details, we refer to the paper.

---

#### Run on Kubernetes
You need to create an account to use the Common Workflow Scheduler in Kubernetes.
Therefore, create a file `account.yaml` with the following content.
Afterward, apply this to your Kubernetes cluster `kubectl apply -f account.yaml`
```
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cwsaccount

---

kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: cwsrole
rules:
 - apiGroups: [""]
   resources: ["pods","pods/status","pods/exec","nodes","bindings","configmaps"]
   verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
 - apiGroups: ["metrics.k8s.io"]
   resources: ["nodes"]
   verbs: ["get","list"]

---

kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
   name: cwsbinding
subjects:
 - kind: ServiceAccount
   name: cwsaccount
   namespace: <your namespace>
roleRef:
   kind: ClusterRole
   name: cwsrole
   apiGroup: rbac.authorization.k8s.io
```

Next, you can start the Common Workflow Scheduler in your Kubernetes environment. 
Nextflow will start the Common Workflow Scheduler automatically.
Therefore, create a file `cws.yaml` with the following content.
Afterward, apply this to your Kubernetes cluster `kubectl apply -f cws.yaml`

```
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: cws
    component: scheduler
    tier: control-plane
  name: workflow-scheduler
spec:
  containers:
  - env:
    - name: SCHEDULER_NAME
      value: workflow-scheduler
    - name: AUTOCLOSE
      value: "false"
    image: commonworkflowscheduler/kubernetesscheduler:v1.0
    imagePullPolicy: Always
    name: workflow-scheduler
    resources:
      limits:
        cpu: "2"
        memory: 1400Mi
      requests:
        cpu: "2"
        memory: 1400Mi
    volumeMounts:
    - mountPath: /input # mount at the same path as you do in your workflow
      name: vol-1
    - mountPath: /data
      name: vol-2
  securityContext:
    runAsUser: 0
  serviceAccount: cwsaccount # use the account created before
  volumes:
  - name: vol-1
    persistentVolumeClaim:
      claimName: api-exp-input # mount the same pvc as you use in your workflow.
  - name: vol-2
    persistentVolumeClaim:
      claimName: api-exp-data
```

#### Profiles
This is a Spring Boot application, that can be run with profiles. The "default" profile is used if no configuration is set. The "dev" profile can be enabled by setting the JVM System Parameter

        -Dspring.profiles.active=dev
or Environment Variable

        export spring_profiles_active=dev
or via the corresponding setting in your development environment or within the pod definition.

Example:

        $ SCHEDULER_NAME=workflow-scheduler java -Dspring.profiles.active=dev -jar cws-k8s-scheduler-1.2-SNAPSHOT.jar

The "dev" profile is useful for debugging and reporting problems because it increases the log-level.

---
#### Memory Prediction and Task Scaling
- Supported if used together with [nf-cws](https://github.com/CommonWorkflowScheduler/nf-cws) version 1.0.4 or newer.
- Kubernetes Feature InPlacePodVerticalScaling must be enabled. This is available starting from Kubernetes v1.27. See [KEP 1287](https://github.com/kubernetes/enhancements/issues/1287) for the current status.

The memory predictor that shall be used for task scaling is set via the nf-cws configuration. If not set, task scaling is disabled.

| cws.memoryPredictor | Behaviour                                                                                                                          |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------|
| ""                  | Disabled if empty or not set.                                                                                                      |
| none                | NonePredictor, will never make any predictions and consequently no task scaling will occur. Used for testing and benchmarking only.|
| constant            | ConstantPredictor, will try to predict a constant memory usage pattern.                                                            |
| linear              | LinearPredictor, will try to predict a memory usage that is linear to the task input size.                                         |
| combi               | CombiPredictor, combines predictions from ConstantPredictor and LinearPredictor.                                                   |
| wary                | WaryPredictor, behaves like LinearPredictor but is more cautious about its predictions.                                            |
| default             | Query the environment variable "MEMORY_PREDICTOR_DEFAULT" and use the value that is set there.                                     |

If a memory predictor is selected (i.e. setting is not disabled), the implementation will locally record statistics and print out the result after the workflow has finished. This can be disabled via the  environment variable "DISABLE_STATISTICS". If this is set to any string, the implementation will not collect and print out the results.

---

If you use this software or artifacts in a publication, please cite it as:

#### Text
Lehmann Fabian, Jonathan Bader, Friedrich Tschirpke, Lauritz Thamsen, and Ulf Leser. **How Workflow Engines Should Talk to Resource Managers: A Proposal for a Common Workflow Scheduling Interface**. In 2023 IEEE/ACM 23rd International Symposium on Cluster, Cloud and Internet Computing (CCGrid). Bangalore, India, 2023.

#### BibTeX
```
@inproceedings{lehmannHowWorkflowEngines2023,
 author = {Lehmann, Fabian and Bader, Jonathan and Tschirpke, Friedrich and Thamsen, Lauritz and Leser, Ulf},
 booktitle = {2023 IEEE/ACM 23rd International Symposium on Cluster, Cloud and Internet Computing (CCGrid)},
 title = {How Workflow Engines Should Talk to Resource Managers: A Proposal for a Common Workflow Scheduling Interface},
 year = {2023},
 address = {{Bangalore, India}},
 doi = {10.1109/CCGrid57682.2023.00025}
}
```
---
#### Acknowledgement:
This work was funded by the German Research Foundation (DFG), CRC 1404: "FONDA: Foundations of Workflows for Large-Scale Scientific Data Analysis." 