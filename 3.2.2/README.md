# wordcount
#### scala版代码：lab3.scala
#### java版代码：Main.java
#### 任意运行一个即可
scala 使用run_new.sh运行  
java 使用run.sh运行

### 可能的报错

1.Exception in thread "main" java.lang.NoSuchMethodException: akka.remote.RemoteActorRefProvider.<init>(java.lang.String, akka.actor.ActorSystem$Settings, akka.event.EventStream, akka.actor.Scheduler, akka.actor.DynamicAccess)
##### 解决方案：网上找akka-actor_2.10-2.3.16.jar替换scala/lib中的akka-actors.jar

2.Exception in thread "main" java.lang.NoSuchMethodError: com.typesafe.config.Config.getDuration(Ljava/lang/String;Ljava/util/concurrent/TimeUnit;)

##### 解决方案：同上，从网上搜scala config,找config-1.2.1替换typesafe-config.jar

##### 网址：https://mvnrepository.com/


###### to 环境安装文档：我测你们码