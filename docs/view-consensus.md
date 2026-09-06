# Block / ViewTimeout 投票

本次只扩展 finality target 和原有投票流程，不增加兼容层、签名格式、计时调度、
择链策略或持久化格式。这些职责继续由现有 Environment 提供。

## 目标模型

目标之间具有单父节点的祖先关系；由业务侧存储，并通过已有 `Chain` 接口提供：

```rust
ConsensusTarget {
    parent: Option<TargetId>, // 只有约定的根目标为 None
    view: u64,
    kind: TargetKind::Block(BlockRef { hash, number }),
    // 或 TargetKind::ViewTimeout
}
```

- Block：view +1，真实 block number +1。
- Timeout：view +1，继承父节点最后一个真实区块。
- 同父节点、同 view 的 Block 和 Timeout 是竞争分支。
- Timeout 最终化后，该 view 的竞争区块不能再进入最终化分支。

例如 `Block(v1,n1) -> Timeout(v2) -> Timeout(v3) -> Block(v4,n2)`：
共识高度是 1、2、3、4，真实块高度是 1、1、1、2。祖先关系必须包含中间的 Timeout。

`TargetId` 根据链域以及目标的 parent、view、类型和 payload 确定性计算。
Timeout 不能复用上一个真实区块的 hash，否则会合并不同的投票对象。

## 核心接口

```rust
// H 是共识目标身份，N 是 view，不是真实区块的 hash/number。
enum VoteTarget<H, N> {
    Block(H, N),
    ViewTimeout(H, N),
}
```

- `Chain::vote_target(hash)` 返回已知目标的类型、身份和 view。
- `Environment::BestChain` 返回 `Result<Option<VoteTarget<H,N>>, Error>`。
- 原 `Environment::finalize_block` 改为 `finalize_target`，接收共识目标及其证明。

使用这些目标类型时，Voter/Round/VoteGraph 的 `H = TargetId`、`N = View = u64`。
原有消息字段 `target_hash` 表示目标身份，`target_number` 表示 **view**；
真实区块 hash/number 保存在 Block payload 中。

`base`、`prevote_ghost`、`estimate`、`finalized` 都指向共识目标。
GRANDPA 的 round 与 view 独立：每轮仍只发一次 prevote、一次 precommit，
precommit 仍由 prevote-GHOST 决定，不增加 Timeout 投票阶段。

## Environment 的职责边界

什么时候生成 Timeout、等待多久、选择哪个目标，仍在现有
`best_chain_containing` 实现中决定。库只消费其结果，不实现另一套 view timer 或择链规则。
等待目标时 Future 保持 Pending；`None` 表示 base 无法使用，不表示 Timeout。

目标及祖先必须在业务侧可查询后，再投递投票消息。核心检查目标身份/view 和祖先关系；
真实区块有效性、Timeout 准入、网络和签名验证仍由 Environment 处理。

`finalize_target` 对 Block 和 Timeout 都会回调，即使真实区块高度没有变化。
业务侧在该回调中更新共识最终性及对应的真实区块最终性。
最终化 Timeout 也可能同时最终化它前面的真实区块，不能只检查当前目标的类型。

持久化和恢复继续由 Environment 实现，本库不定义快照格式。
需要恢复原始目标身份、祖先关系及已最终化的 target/view，不能只保存最后一个真实区块。
正式代码不提供 `ViewChain` 等内存存储实现；需要的模拟实现只定义在测试模块中。

## 验证

```sh
cargo test --all-features
cargo test --no-default-features
```

`src/testing/view_chain.rs` 定义测试专用的模拟链，
`src/testing/view_targets.rs` 覆盖原有 Round/Voter 的混合目标投票与最终化。
这些模块仅在 `#[cfg(test)]` 下编译，不对外导出。
