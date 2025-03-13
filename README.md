# Kubernetes Workflow Scheduler

SWAGGER: http://localhost:8080/swagger-ui.html

API-DOCS: http://localhost:8080/v3/api-docs/

---
#### Build
```
docker build -t cws .
docker tag cws <your docker account>/cws:<version>
docker push <your docker account>/cws:<version>
```

#### API Endpoints

| #  | Resource                                           | Method |
|----|:---------------------------------------------------|:------:|
| 1  | /{version}/scheduler/{execution}                   |  POST  |
| 2  | /{version}/scheduler/{execution}                   | DELETE |
| 3  | /{version}/scheduler/{execution}/DAG/vertices      |  POST  |
| 4  | /{version}/scheduler/{execution}/DAG/vertices      | DELETE |
| 5  | /{version}/scheduler/{execution}/DAG/edges         |  POST  |
| 6  | /{version}/scheduler/{execution}/DAG/edges         | DELETE |
| 7  | /{version}/scheduler/{execution}/startBatch        |  PUT   |
| 8  | /{version}/scheduler/{execution}/endBatch          |  PUT   |
| 9  | /{version}/scheduler/{execution}/task/{id}         |  POST  |
| 10 | /{version}/scheduler/{execution}/task/{id}         |  GET   |
| 11 | /{version}/scheduler/{execution}/task/{id}         | DELETE |
| 12 | /{version}/scheduler/{execution}/metrics/task/{id} |  POST  |

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
    image: commonworkflowscheduler/kubernetesscheduler:v2.0
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

        $ SCHEDULER_NAME=workflow-scheduler java -Dspring.profiles.active=dev -jar cws-k8s-scheduler-2.0.jar

The "dev" profile is useful for debugging and reporting problems because it increases the log-level.

---
#### Memory Prediction and Task Scaling
- Kubernetes Feature InPlacePodVerticalScaling must be enabled. This is available starting from Kubernetes v1.27. See [KEP 1287](https://github.com/kubernetes/enhancements/issues/1287) for the current status.
- Supported if used together with [nf-cws](https://github.com/CommonWorkflowScheduler/nf-cws) version 1.0.5 or newer.

The memory predictor that shall be used for task scaling is set via the configuration. If not set, task scaling is disabled.
The memory predictor is provided as a string following the pattern "&lt;memory predictor&gt;-[&lt;additional&gt;=&lt;parameter&gt;]", e.g., "linear-offset=std".
The following strategies are available:

| Memory Predictor | Behaviour                                                                                                                                                                               |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| linear/lr        | The Linear predictor, will try to predict a memory usage that is linear to the task input size.                                                                                         |
| linear2/lr2      | The Linear predictor with an unequal loss function. The loss penalizes underprediction more than overprediction.                                                                        |
| mean             | The Mean predictor predicts the mean memory seen so far. Prediction is independent of the input size.                                                                                   |
| ponder           | The Ponder predictor is an advanced memory prediction strategy that ponders between linear regression with unequal loss and historic values. Details are provided in our paper [https://arxiv.org/pdf/2408.00047.pdf](https://arxiv.org/pdf/2408.00047.pdf). |
| constX           | Predicts a constant value (X), if no X is given, it predicts 0.                                                                                                                         |
| polyX            | Prediction will be based on the Xth polynomial function based on a task's input size. If no X is provided, it uses X=2.                                                                 |


The offset uses the current prediction model and based on that it predicts the memory for all finished tasks.
Then, it calculates the difference between the observed memory and the predicted memory.

| Offset      | Behaviour                                                                                                     |
|-------------|---------------------------------------------------------------------------------------------------------------|
| none        | No additional offset will be applied.                                                                         |
| ""          | If no offset is defined, the max offset will be used.                                                         |
| max         | The max offset returns the largest underprediction.                                                           |
| Xpercentile | X is an integer between 1 and 100, over all prediction differences, it will use the Xth percentile as offset. |
| var         | This offset applies the variance as an offset.                                                                |
| Xstd        | This offset applies X times the standard deviation as an offset. If no X is provided, it uses X=1.            |

#### Scheduling strategies

The scheduling strategy can be set via the configuration.
The scheduling strategy is provided as a string following the pattern "&lt;scheduling strategy&gt;[-&lt;node assignment strategy&gt;]".
The following strategies are available:

| Scheduling Strategy | Behaviour                                                                                                                                                |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| fifo                | Tasks that have been submitted earlier, will be scheduled earlier.                                                                                       |
| rank                | Tasks will be prioritized based on their rank in the DAG.                                                                                                |
| rank_min            | Rank (min) Same as rank but solves ties such that tasks with smaller input size are preferred.                                                           |
| rank_max            | Rank (max) Same as rank but solves ties such that tasks with larger input size are preferred.                                                            |
| lff_min             | Least finished first (min): prioritizes abstract tasks where less instances have finished, solves ties with rank_min                                     |
| lff_max             | Least finished first (max): prioritizes abstract tasks where less instances have finished, solves ties with rank_max                                     |
| gs_min              | Generate Samples (min) Hybrid of LFF (min) and Rank (max), prioritize abstract tasks with less than five finished instances. Afterwards, use Rank (max). |
| gs_max              | Generate Samples (max) Hybrid of LFF (max) and Rank (max), prioritize abstract tasks with less than five finished instances. Afterwards, use Rank (max). |
| random              | Randomly prioritize tasks.                                                                                                                               |
| max                 | Prioritize tasks with larger input size.                                                                                                                 |
| min                 | Prioritize tasks with smaller input size.                                                                                                                |
| wow                 | WOW scheduler for data location awareness. This is scheduling + node assignment. Details are provided in our paper [tbd](tbd).                           |

| Node Assignment Strategy | Behaviour                                                                               |
|--------------------------|-----------------------------------------------------------------------------------------|
| random                   | Randomly distributes the tasks to nodes.                                                |
| roundrobin               | (default) Assigns tasks in a round robin fashion to the nodes.                          |
| fair                     | Distributes the tasks fairly to the nodes trying to achieve equal load on all machines. |

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
