# pairwise-quest-objective-settle

## 领域

游戏、图形与多媒体

## 主题

任务目标进度结算

## 说明

事件去重、监听生命周期与离线补算顺序。

初始状态下 `gameplay/quest_objective_settle.py` 中所有函数抛出 `NotImplementedError`，测试全部失败。
修复实现后运行：

```bash
python -m pytest -q
```

期望全部通过。
