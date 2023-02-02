# Common Workflow Scheduler for Kubernetes

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
API-DOCS: /v3/api-docs/

For more details, we refer to the paper.

---

If you use this software or artifacts in a publication, please cite it as:

#### Text
Lehmann, Fabian, Jonathan Bader, Friedrich Tschirpke, Lauritz Thamsen, and Ulf Leser. **How Workflow Engines Should Talk to Resource Managers: A Proposal for a Common Workflow Scheduling Interface**. In 2023 23rd IEEE International Symposium on Cluster, Cloud and Internet Computing (CCGrid). Bangalore, India, 2023.

#### BibTeX
```
@inproceedings{lehmannHowWorkflowEngines2023,
 author = {Lehmann, Fabian and Bader, Jonathan and Tschirpke, Friedrich and Thamsen, Lauritz and Leser, Ulf},
 booktitle = {2023 23rd IEEE International Symposium on Cluster, Cloud and Internet Computing (CCGrid)},
 title = {How Workflow Engines Should Talk to Resource Managers: A Proposal for a Common Workflow Scheduling Interface},
 year = {2023},
 address = {{Bangalore, India}}
}
```
---
#### Acknowledgement:
This work was funded by the German Research Foundation (DFG), CRC 1404: "FONDA: Foundations of Workflows for Large-Scale Scientific Data Analysis." 