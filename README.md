A simple test of akka streams designed to demo the concept of "backpressure". 

## Real-life use case
Although this is just a sample demo, I've used the same framework in production. For example, when building a system that drew content from multiple microservices, we used this technique to ensure that the entire pipeline respected the pace of the slowest microservice. For example, one service would enevitably hit its threshold CPU limit before another. Although they scale horizontally, it takes a few minutes to spin up a new VM (and of course there is an upper scaling limit). Using Akka streams this way ensured that the entire system could slow down and wait for constrained services to get their capacity back up.

## Run the app
1. Start docker & a terminal inside container
  - `docker run -it -v "$PWD":/app -w /app --name hellostreams-dev hellostreams bash`
2. Run the app: 
  - `sbt run`
3. Edit HelloStreams.scala and re-run the app

```
$ docker run -it -v "$PWD":/app -w /app --name hellostreams-dev hellostreams bash

root@1c26944d9509:/app# sbt run
[info] welcome to sbt 1.10.11 (Oracle Corporation Java 1.8.0_342)
[info] loading settings for project app-build from assembly.sbt...
[info] loading project definition from /app/project
[warn] sbt-assembly.jar no longer exists at /root/.cache/coursier/v1/https/repo.scala-sbt.org/scalasbt/sbt-plugin-releases/com.eed3si9n/sbt-assembly/scala_2.12/sbt_1.0/0.14.10/jars/sbt-assembly.jar
[info] Updating app-build
[info] running Main
1
2
3
4
5
6
7
8
9
10
...completed simpleStream()

[success] Total time: 11 s, completed May 29, 2025 2:43:16 PM
root@1c26944d9509:/app# 
```