# yarn-application-flink-windows
适合在Windows上提交任务的flink-yarn包

## 调整说明

> 1. 修改 YarnClusterDescriptor.java和Utils.java ,将分隔符替换为linux/unix环境年分隔符
> 2. 配置好yarn-site.xml中的yarn.application.classpath
> 3. 调整个各个配置文件中的hadoop server的ip地址
> 4. 通过examples中的例子进行验证

**调整代码如下**
```java
// FIX env from window submit to linux/unix
val = val.replaceAll(";",":").replaceAll("::",":");
```