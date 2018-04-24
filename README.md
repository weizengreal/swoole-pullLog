# swoole-pullLog

## 简介(Introduction)

swoole-pullLog 是一个基于 swoole、阿里云日志服务的消息同步微服务，用于多语言、多业务端之间的解耦；
多个端上传日志到阿里云日志服务之后可以该服务轻松进行数据落地。

问题提交：[Issue](https://github.com/weizengreal/swoole-pullLog/issues)

协议支持：[Apache-2.0](https://github.com/weizengreal/swoole-pullLog/blob/master/LICENSE)

## 设计思路(Design)

> * [系统架构设计](http://blog.woai662.net/?p=39)
> * [日志拉取demo实现](http://blog.woai662.net/?p=41)
> * [完成版实现简介](http://blog.woai662.net/?p=43)


## 功能支持(Function)

> * 底层封装了对多进程的管理
> * puller 与 consumer 之间通过共享内存队列通信
> * 上层只需实现 filter 和 batchConsume 开箱即用
> * 其他相关知识请参考[Swoole扩展](https://wiki.swoole.com/)


## 安装依赖(Depend)

> * Swoole 1.9.x+
> * Php 5.6+
> * Pcntl 扩展


## 使用方法(Example)

```
1、在 config/cfg_aliyun.php 中修改阿里云日志服务相关的配置；
2、在 config/cfg_aliyun.php 中添加 aliyun 和 vaildStore 相关配置；
3、实现 puller 的 filter 方法和 consumer 的 batchConsume 方法；
4、运行 start.sh 即可；
```