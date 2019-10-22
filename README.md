# task-manage
该模型基于rocketmq 可以扩展到其他类似任务处理。
使用策略模式 将任务管理 单独 抽出来一个模块。真实应用程序只需实现任务处理逻辑 返回成功失败

![image](https://github.com/zengxiaobai/task-manage/blob/master/images/taskmanager.png)

#### task-manage 线程  

1. 负责接收消息，将消息存储到 对应客户的task-list 链表中
2. 周期性处理任务，每次处理遍历客户map，从客户map的task-list 中选取一个头部task。将挑选的task 交由 task-deal 处理线程池，，实现公平调度
3. 接收task-deal 处理线程的处理结果，是否需要失败重试，如果需要则将task 从当前位置移动至list末尾。如果处理成功从客户map 所属的task-list中删除节点。
4. 接收退出信号，持久化task-list。由于只有task-deal 线程 执行完成后 才会 删除task节点。即使正在处理消息的task-deal 线程 被强制退出， 也可以保证不丢失消息(消息持久化磁盘)

#### task-deal 线程池

1. 负责接收task-manage  传递进来的任务，进行处理
2. 负责反馈处理结果给task-manage 线程

#### 任务管理策略模式

**ms 模式**

上述 task-manage 线程在 周期性处理任务过程中 ，每次遍历map 客户时 一定会选择一个该客户的task list 的一个task处理。（task-list 为空时 删除map中客户）。当可用task-deal gorutines 为0 时，会因等待有可用的task-deal gorutines 阻塞 而不消费新的消息。

该模式的可能会导致客户间相互影响，当某个时刻 只有某个客户的大量task ，占用 了所有 task-deal gorutines。且每个任务处理周期很长时，其他客户的消息得不到消费。

ms 意为着以可以处理的最大max-gorutines 为主的一种strategy模式，适合任务处理较块的场景 如缓存推送。没有限制单客户占用的task-deal gorutines数量。

**rs 模式**

与ms 模式相比，限制了单个客户的占用task-deal gorutines。限制条件为：当前客户正在处理的任务数 不能 大于 可用的 闲置task-deal gorutines 数。

上述task-manage 线程 周期性处理任务过程中，每次处理过程 遍历客户map，从客户map的task-list 中选取一个头部task  交由 task-deal 处理线程池，每次迭代处理 每个客户 最多 会被选取一个task进行处理：

1. 如果当前客户的正在处理任务数 多于 剩余闲置的 task-deal 线程数，则迭代遍历任务时不选取当前客户的task，继续处理选择下一个客户的task。避免上述某个时刻 某个客户占满了 task-deal 线程池且占用时间较久导致其他客户无法被服务。也就是说，下一个客户永远有闲置的线程可以用.直到达到最大并发数。达到最大并发数时，可用task-deal gorutines 为0 时，会因等待有可用的task-deal gorutines 阻塞 而不消费新的消息。

当 最大并发数未满足，而某个客户却达到了 自身可用的 task-deal gorutines时，会继续接收消息，（并不会妨碍 其他客户的服务），但因此 也可能收到该客户自身的新消息，此时只能继续缓存消息，将该消息 加入 客户的task-list 中。但是因为内存限制不可能无限制加入。如果task-list 达到 FetchReconsumeListLen （PushReconsumeListLen）长度时，会通知rocketmq-server 延迟消费（ReconsumerLater）, 因此称为Reconsume strategy，该模式较适合 预取 等时间较长的任务。经测试 150w 占用内存300M.

**其他未实现模式**

在rs 模式的基础上不做Reconsume  Later 限制，只要是消息就收取，list达到一定长度后 加入数据库（sqlite），另开线程 从sqlite 读取，并发送消息给 taskmanage 线程 删除。该模式在不支持 Reconsume 的消息中间件中必须使用。

本次实现任务管理策略的过程中 使用了 策略模式的设计模式 且 使用channel 进行数据流传输 非常方便扩展。

