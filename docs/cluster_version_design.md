# rkyv、统一 Hello 与滚动升级计划

日期：2026-10-01。状态：RPC 准入、旧版本机制删除和运维流程已实现；尚未部署。首次 stopworld 与后续真实双 build 的滚动升级仍需发布演练。

## 本次实现的三项变化

1. **RPC 层显式版本检查**：每条连接在业务 decode 前完成 Hello；内部/admin 使用 WIRE_VERSION 相等检查，client 使用现有 [MIN_CLIENT_WIRE_VERSION, WIRE_VERSION] 区间检查。版本不匹配明确返回双方版本及拒绝原因，与传输错误、身份错误和业务 decode 错误区分；不是等 decode 失败后猜测是否在升级。
2. **删除 cluster_version 和 cluster bump 机制**：删除 manager 的 etcd 版本 latch、启动/replay 检查、查询与 bump RPC，以及 autumn-op 的 cluster-version、upgrade-version 命令。不再以 cluster_version 判断持久化兼容或限制回滚；持久化变更按每次发布单独处理。冻结响应中的历史字段不改变已有编码，退役后仅作保留占位；旧 opcode 不复用，cluster_id 和 ownership generation 保留。
3. **更新 docs/ops.md 的一般升级流程**：记录原 policy 并切换为 Off，停止新的管理任务，等待 compaction、EC、GC、split/merge、rebalance 等已派发任务结束，核查 recovery 和 PS 本地后台任务；再 drain、替换、验证恢复，最后恢复原 policy。首次统一 Hello 使用已确定的 stopworld；后续在持久化兼容、现有 fencing/恢复机制保证已 ACK 数据不丢失的条件下，允许有 RPC 失败、超时、结果未知和短暂不可用的 rolling upgrade，wire 改变也适用。

后文是这三项变化的实现与验收细节；保留 rkyv、现有客户端兼容区间，以及持久化按次分析的方式。

## 1. 决策与范围

1. RPC 保留 rkyv，不迁移 Protobuf，不新增 proto/protoc。
2. 所有业务 RPC 连接先完成稳定的 bootstrap Hello。内部 peer 和 admin 要求 WIRE_VERSION 相等；客户端按 client_wire_version 兼容规则准入。
3. 连接准入与请求准入都在必经入口执行。通过客户端兼容窗口不代表可以调用内部或 admin RPC。
4. **首次上线统一 Hello 采用 stopworld**：同步更新 manager、PS、EN、SDK、autumn-op 及其他后台调用方，关闭旧连接。不要求这次迁移兼容无 Hello 的旧连接；保留现有 WIRE_VERSION 与 MIN_CLIENT_WIRE_VERSION 及客户端区间准入机制。
5. **WIRE_VERSION 改变也可以 rolling upgrade**：不同 wire 的进程可以在逐实例替换期间暂时并存，但异 wire 内部 RPC 一律在 Hello 阶段拒绝，不解码或执行业务。不支持跨 wire 互通，也不引入双 codec；混合阶段允许 RPC 失败、超时和业务暂时不可用，最终收敛为同一 wire。持久化是否允许滚动升级按具体改动单独判断。
6. 持久化格式改动很少，涉及改动时逐次分析受影响的 reader/writer、格式与恢复语义，单独确定转换与停机范围；本计划不建设统一持久化 read/write 入口，也不引入统一 cluster_version/storage epoch gate。

本文不采用 Protobuf 或统一 storage epoch。首次握手机制迁移按已确定的 stopworld 执行；机制落地后，wire 改变本身不要求 stopworld。持久化变化仍逐次处理，必要时可要求停写或 stopworld。

## 2. 版本的职责

| 项目 | 职责 | 变更规则 |
|---|---|---|
| WIRE_VERSION | 内部及 admin RPC 的布局、编码和协议语义硬边界；也是当前客户端协议上界 | 改变布局或不兼容语义时 bump；不是每次发布都 bump |
| client_wire_version | 客户端连接声明的协议版本 | 按客户端兼容规则判断；不能代替内部 peer exact-match |
| MIN_CLIENT_WIRE_VERSION | 当前实现真实支持的客户端协议下界 | 保留现有下界；只有实际收紧客户端兼容范围时才调整，不能因服务端发布或内部 wire bump 自动提高 |
| bootstrap 协议标识/版本 | 业务版本检查之前的固定握手格式 | 独立于业务 DTO 冻结；未知 bootstrap 版本拒绝，不尝试业务解码 |
| 现有格式版本（如有） | 具体持久化格式的解释依据 | 涉及该格式改动时按本次迁移方案处理，不要求统一补齐版本或 envelope |

rkyv 不提供 Protobuf 式字段增补兼容：新增 Option 字段也可能改变 archived layout，需要判断并 bump wire。普通优化、修复和未改变协议的发布不 bump。客户端 surface 改变时须保留真实旧入口/类型，或明确收紧兼容范围；不能仅用整数窗口声称兼容。

当前 WIRE_VERSION = 52、MIN_CLIENT_WIRE_VERSION = 43。新增 Hello 不自动 bump wire 或提高客户端下界；业务协议确有变化时，再按相应变更规则处理。

客户端连接时使用现有区间检查：服务端提供 [MIN_CLIENT_WIRE_VERSION, WIRE_VERSION]，客户端声明的 client_wire_version 必须位于该区间。当前 SDK 使用自身 WIRE_VERSION 作为声明值。这是客户端所用协议与服务端支持集合有交集；不把内部 peer 的准入改成区间 overlap。内部成员只使用 WIRE_VERSION exact-match。

“内部不允许混合 wire”指不允许不同 wire 之间建立可用业务 RPC 连接，不要求部署中的全部进程始终只有一种 wire。升级期间 manager、PS、EN 可以存在不同 wire 的进程；它们只与相同 wire 的对端通信。异 wire 的注册或业务请求被拒绝，依赖未满足的节点保持等待，最终全体收敛。同 wire 的不同 build/commit 不因源码差异被拒绝，客户端兼容区间也不参与内部 peer exact-match。

统一 Hello 是连接准入的另一个条件。首次 stopworld 更新旧 SDK 的握手实现，不借此自动收紧其业务协议支持区间；仍保留区间内实际支持的客户端入口、类型与行为。区间声明不能替代必需的 Hello，也不能单独证明未更新握手的历史 binary 可以连接。

现有 MSG_CLIENT_HELLO、GetClusterIdResp 等冻结编码不直接加字段、改变含义或复用为新的 role 编码。新 bootstrap 使用独立、明确的标识；旧 Hello 不能使未校验连接成为可用业务连接。保留历史字段不等于继续接受旧握手。

删除现有 etcd cluster_version latch 的读写、启动/replay 检查、查询/bump RPC 和 cluster-version、upgrade-version 命令（cluster bump）。GetClusterIdResp 等冻结编码中的历史字段保持占位，不再承载有效版本状态；退役 opcode 保留编号，不复用。遗留 etcd key 不再读取或影响行为，清理可单独安排。不得误删 cluster_id/ownership generation，也不能因为移除旧 latch 就默认旧 build 可安全回滚。

## 3. 已接入的连接路径

- manager、PS、EN 的 listener 均先完成 `version_hello::accept`，再对 Peer / Admin 连接完成 `peer_auth::accept`（集群密钥，见 `cluster_secret_design.md`），然后才创建业务 `FrameDecoder`。
- `RpcClient::connect` / `from_conn` 在启动业务 reader/writer 前完成两步握手（VERSION_HELLO，Peer / Admin 再加 PEER_AUTH）。默认 peer；SDK 使用 client，autumn-op 使用 admin，EN 身份检查和注册显式使用 peer。
- `ConnPool` 的角色在构造时固定；失败握手不会进入连接池。SDK 到 EN 的 direct-read 池独立使用 client。
- manager、PS、EN 在分派或批量 append 合并前执行 role × opcode 检查。role 只是声明，Peer / Admin 由 PEER_AUTH 证明；capability（AUTH_HELLO）、cluster_id、ownership 检查继续执行。
- PS 获取 owner lock 和注册、EN 校验 manager 身份和注册，在明确的 wire mismatch 或暂时传输故障时等待重试，不跳过版本检查。身份、授权、存储异常仍返回错误。

## 4. 连接与请求的统一准入

### 4.1 bootstrap 的冻结边界

统一版本握手命名为 VERSION_HELLO，常量 MSG_VERSION_HELLO = 0xF0（manager、PS、EN 共用）。使用新 magic AUPH，bootstrap_version 初始为 1；既有 MSG_CLIENT_HELLO = 0x5F 的冻结编码不修改。VERSION_HELLO 使用独立冻结的固定格式解析，不依赖 rkyv 或业务 FrameDecoder。

AUTH_HELLO = 0x55 是既有 SDK → PS 认证消息，仍使用现有 rkyv 编码：请求 AuthHelloReq { token: Vec<u8> }，响应 AuthHelloResp { code: u8, message: String }。客户端连接先完成 VERSION_HELLO，再按现有认证配置执行 AUTH_HELLO（PS；开启 authz 时也包括 EN 的直读连接）。Peer / Admin 连接在 VERSION_HELLO 之后执行 PEER_AUTH（MSG_PEER_AUTH = 0xF1，与 VERSION_HELLO 同一冻结帧格式），证明持有集群密钥。版本 Hello 的 role 仅作协议声明，不授予身份或权限。

bootstrap 使用固定二进制格式，包含协议标识/版本、连接角色、声明的 wire/client 版本。请求、成功响应和拒绝响应都不能依赖可变的 rkyv DTO。响应明确携带目标服务类型、服务端 wire、客户端兼容范围和失败原因，连接发起方也必须校验，不能仅依赖 listener 单向判断。

冻结范围覆盖 bootstrap 外层 framing、字段宽度、字节序、长度上限和错误结果，而不只是 Hello payload。握手不得依赖可能随业务 wire 改变的 FrameDecoder 才能读出版本。当前实现的固定格式如下（整数均为 little-endian）：

外层：`req_id:u32=1 | opcode:u8=0xF0 | flags:u8 | payload_len:u32 | ctrl_len:u32 | ctrl | crc32c:u32`。
`flags` 请求为 0、响应为 1；`payload_len=ctrl_len+8`；CRC 覆盖 header、ctrl_len 与 ctrl，无 value tail。
这一 framing 单独实现，不随业务 Frame/FrameDecoder 改变。请求总长 34 字节，响应总长 40～296 字节。

| ctrl | 字段，按发送顺序 |
|---|---|
| 请求，16 字节 | `magic[4]=AUPH, bootstrap_version:u16=1, role:u8, reserved:u8=0, wire_version:u32, client_version:u32` |
| 响应，22+n 字节 | `magic[4]=AUPH, bootstrap_version:u16=1, verdict:u8, service:u8, wire_version:u32, min_client:u32, max_client:u32, message_len:u16, message[n]` |

角色：client=1、peer=2、admin=3；目标服务：manager=1、PS=2、EN=3。
结果：成功=0、wire 不匹配=1、client 不匹配=2、malformed=3、bootstrap 不匹配=4。
peer/admin 的 client_version 必须为 0；客户端以 client_version 判断区间。原因消息最多 256 字节。
握手及 RpcClient 的 connect+握手上限为 5 秒；调用方更短的预算优先。请求/成功响应固定字节、角色/版本边界和异常 framing 已有测试。

握手使用独立长度上限和超时；长度在读取或分配 body 前检查。未知协议、非法 role、malformed、无 Hello、无法获取版本或超时均拒绝并关闭连接，不通过 decode 失败猜测版本，不回退到无校验模式。

cluster_id/auth/ownership 校验继续独立执行；Hello 中的 role 是协议声明，不是身份或授权证明。只有完成所需的版本和身份校验后，连接才成为可用于业务的已校验连接。

### 4.2 角色与请求准入矩阵

| 连接角色 | 连接版本规则 | 允许的请求 |
|---|---|---|
| client | MIN_CLIENT_WIRE_VERSION ≤ client_wire_version ≤ 服务端 WIRE_VERSION | 明确列入客户端兼容 surface 的请求；按声明版本选择对应 codec/行为 |
| peer（manager/PS/EN） | 本地与对端 WIRE_VERSION 相等 | 对应 peer 类型允许的内部请求，以及明确允许的共享请求 |
| admin（autumn-op 等） | 本地与对端 WIRE_VERSION 相等 | 明确列入 admin surface 的请求，以及明确允许的共享请求；授权仍按现有规则执行 |

已校验连接记录本地及远端服务类型、角色、版本和适用兼容规则。连接角色建立后不可更换；重复 Hello 不得重置准入状态，拒绝后不能再尝试另一角色进入业务。

统一分派入口在解码业务 body 前检查 role × opcode × declared version。client 连接不能调用注册、内部 mutation 或 admin 请求；peer 连接不能自动获得 admin 权限。共享 opcode 必须显式登记可用角色及其布局/语义兼容范围，不能简单把所有共享请求都当作内部请求拒绝旧客户端。

EN 注册与 startup identity check 显式使用 peer 模式；autumn-op 显式使用 admin 模式。普通 SDK 保持 client 模式。同一库供多个调用方使用时，通过构造入口和类型显式区分，不以库名或进程名推断角色。

请求、响应、节点直连、池连接、批量、streaming、后台任务和 admin 路径都经过此入口。业务裸 codec 和原始连接分派限定在基础设施模块；业务 send/receive 只接受已校验连接。客户端历史 codec 依据已校验的声明版本选择，不能由业务 handler 自行猜测版本。

### 4.3 SDK → EN direct-read

direct-read 是独立客户端连接：首次建连必须 Hello；EN 按 client_wire_version 与对应客户端读请求的兼容规则准入。不能因为请求绕过 PS、读取的是 bytes 或已经向 manager/PS 做过 Hello 而跳过检查。

重连、连接池替换、换 replica/EN 地址均重新握手。连接池的复用条件至少包含地址、角色与版本/身份上下文，不能把同一地址的 client、peer、admin 连接混用。开启 authz 时，EN 只在 AUTH_HELLO 绑定了有效 principal 的连接上服务直读（只验身份，不验 key 范围，见 `data_plane_authz_design.md` §10）；extent eversion 检查继续执行。

首次发布不以旧 SDK 的 proxy fallback 代替 SDK 更新，也不因此自动提高 MIN_CLIENT_WIRE_VERSION。后续支持窗口内的客户端还须支持统一 Hello；新服务端需保留它实际使用的 direct-read 请求及相关 token/响应编码，或对具体不兼容变更明确收紧范围。

### 4.4 连接生命周期

连接状态为：未校验 → 握手中 → 已校验 → 关闭；拒绝、超时和传输错误进入关闭状态。连接只有在 Hello 成功后才能放入可用连接池或发送业务；不能在握手中排入业务帧。

每次新建连接、断线重连、连接池替换都重新 Hello。版本在连接建立时比较一次，正常消息不逐次查版本或 etcd；framing/length/codec 与请求角色准入仍逐条执行。

普通替换进程会关闭旧 socket，新进程不得继承旧连接的校验状态。mismatch 连接不保留为可用连接；不能复用已编码旧布局的 buffer 发给新连接。重试的编码和发送必须经过新连接的校验与 codec 选择。

## 5. mismatch、启动和请求恢复

内部 wire mismatch 表示当前对端不能通信，关闭连接并明确拒绝。它可以是 wire 滚动升级中的预期暂态；节点可保持已启动、依赖未就绪的等待状态，退避重连，直到对端也升级到相同 wire。异 wire 注册和业务不能放行；不能把重试当作跨 wire 兼容。

等待状态须能通过本地诊断或管理平台观测，不能依赖被拒绝的业务 RPC 才能查看。错误记录双方版本、角色、客户端范围和原因；启动等待日志记录节点/目标、wire、错误链及下一次重试间隔。当前启动重连使用 1～5 秒退避；部署系统负责整体升级超时，后续可按规模加入抖动和专门等待指标。节点的长期等待和升级总超时分开，永久 mismatch 必须被报告，不能无限静默等待。

传输失败、节点重启中的暂时不可达与版本拒绝分别处理，新连接均重新 Hello。身份、授权失败与非法协议单独分类，不能被 wire 等待逻辑掩盖。移除 PS best-effort wire skip；无法完成检查时保持未校验状态，不先注册再等待验证。核查 EN 注册、启动检查和后台 worker 的固定 retry 上限，避免预期 mismatch 等待退化为反复退出重启。

ConnPool 的限时调用以同一预算覆盖 connect、Hello、发送与响应；SDK 的连接初始化受首尝试预算限制。节点长期重连与单次调用分开。SDK 现有 routing retry 仍使用每次尝试的预算及有限次数，本次未新增全操作总 deadline；不能据此承诺整个 SDK 操作在单次 RPC timeout 内结束。取消未来尝试后不得重新排队；已发送业务请求仍可能执行。

Hello 拒绝前未执行业务请求；业务发送后断线仍可能已经执行。对各写入口记录“未发送可重试”“已发送且可去重”“已发送但结果未知”的处理规则。沿用已有幂等/去重能力，不能因版本检查盲目重放 append、管理操作或已超时请求；缺少去重保证的入口返回结果未知或遵循现有查询/恢复流程。

允许丢失的是未完成的 RPC 消息或响应，表现为明确失败、超时或结果未知；已 ACK 的持久化写不能丢失。升级期间节点判死、ownership 转移和后台恢复仍须遵守现有 fencing 与数据保护规则。Hello 拒绝只能证明异 wire 的业务没有被解码或执行，不能单独证明所有控制流程的数据安全。

## 6. 部署方式

### 6.1 首次统一 Hello 上线：stopworld

这次部署覆盖角色准入迁移与 SDK → EN Hello，使用已确定的首次 stopworld，不将其等同于机制落地后的 wire 滚动升级。发布前列出全部调用方，包括 SDK/wheel、FUSE、S3 gateway、autumn-op、定时任务、迁移工具和其他后台进程，并明确停止、更新和恢复责任。

执行步骤在 docs/ops.md 落成可操作清单：

1. 确认服务端、SDK 和工具的新 build 均已准备，现有客户端兼容区间及上线恢复方案已经验收；若涉及 persist 变化，另完成该次迁移验收。
2. 停止新业务和后台管理写入，drain 已接收请求，记录未确定结果的操作；停止全部旧调用方及服务实例，确认旧 socket 已关闭，禁止旧进程自动拉起。
3. 若本次同时涉及持久化布局或语义变化，按第 7 节为这次改动单独确定的停写/fencing、备份与转换顺序执行。stopworld 本身不能证明数据兼容。
4. 部署全部新服务端、SDK 和工具。按演练验证的依赖顺序启动，在流量关闭状态下完成 Hello、身份检查、注册与恢复。
5. 检查 leader、partition、replicas、读写和 EN direct-read；确认无未更新握手的旧调用方重连，且现有客户端区间与实际支持集合一致，再开放业务和后台任务。

上线失败时保持业务关闭，并执行已验证的恢复方案。有持久化变化时，不能直接回滚 binary 或跳过转换；未发生不兼容数据写入时，也要以兼容矩阵和实际状态确认可否整体回滚。

旧 SDK/旧工具遗留进程必须被拒绝并可观测，不能将拒绝解释为需要临时放开校验。现有冻结 Hello/响应编码保留规则不意味着旧进程获得业务准入。

### 6.2 后续升级的选择

| 发布变化 | 部署方式 |
|---|---|
| 同 wire、无持久化不兼容变化 | 正常 drain、逐实例重启和 Ready 等待 |
| wire 改变、无 persist 变化或该次 persist 方案允许混合运行 | 逐实例或按依赖组替换；允许暂时 mismatch、RPC 失败和业务不可用；最终收敛为目标 wire |
| 持久化不允许新旧 writer 共存 | 按本次格式分析停止有关 writer、fencing 与转换；必要时 stopworld |
| bootstrap 冻结边界发生不兼容变化 | 单独制定握手机制迁移方案及调用方更新范围，不能直接套用普通 wire 滚动流程 |

一般升级先记录 policy 的名称和模式，执行 auto-policy deactivate 并确认 Off，停止新的人工/外部后台管理任务，等待已派发的 compaction、EC、GC、split/merge、rebalance 等结束并检查结果，同时核查 recovery 和 PS 本地后台任务。policy Off 不等于所有后台任务都已暂停。确认后执行替换，恢复验证通过再恢复原 policy 名称与模式；详见 docs/ops.md。暂停 policy 与等待任务用于减少升级期间的干扰，不替代持久化兼容分析、drain、ownership fencing 与恢复保证。

后续 wire 改变不自动提高 MIN_CLIENT_WIRE_VERSION。保留客户端 surface 的兼容实现时，区间内客户端可以继续使用；admin 工具随内部 wire 更新。内部协议升级是否需要更新 SDK，依据该次客户端 surface 与握手变化决定，不要求每个内部 wire bump 都重建客户端。

版本拒绝阻止不同布局互相解码，但 retry 不会让不同 wire 的双方互通。同 wire 滚动重启需满足必要依赖和 replica/EC 服务预算；wire 改变的滚动升级明确接受依赖暂时不足造成的不可用，不能承诺无中断或所有请求自动成功。不可用持续时间由实际依赖和部署速度决定，不预设固定上限。

### 6.3 推进条件与最终恢复

同 wire rolling upgrade 采用 drain → 重启 → Hello/注册/恢复 → Ready → 下一实例的流程。每次替换前检查剩余实例和 replica/EC 的服务预算；新实例未 Ready 时停止推进，设置单实例与整体超时，并按本次恢复方案处理。

wire 改变时不能无条件等待业务 Ready：新实例可能必须连接尚未升级的 manager/EN 才能 Ready，部署器等它 Ready 才升级下一实例就会卡住。区分“进程已启动、本地初始化完成并因已知 wire mismatch 等待”与“整体业务 Ready”，按演练确定的条件继续推进后续实例，或协调替换依赖组。身份、存储或其他初始化错误不能被当作预期 mismatch 忽略。

逐个 drain、停止并替换实例，关闭旧连接，新实例重新 Hello。有关 drain 或关闭步骤若因对端 mismatch 不能正常完成，使用该次演练验证的超时与恢复步骤，不能强制继续并假定已提交写一定安全。设置升级总超时、失败报告和恢复方案；防止旧进程意外拉起拖延版本收敛。

具体替换顺序通过真实拓扑验证，不能预设 manager → PS → EN 一定可行。演练观察心跳中断引发的节点判死、partition ownership 迁移、replica 恢复和 GC 行为，明确维护期间是否需要暂停有关控制任务。manager 经 etcd 选举的路径不受 Hello 保护，仍需验证领导权、共享状态和 ownership fencing 在版本切换期间保持正确；不以部署中同时存在不同 wire 的进程作为错误。

所有进程更新后检查 leader、注册、ownership、partition、replicas/EC 和业务恢复，验证已 ACK 写入可读且无错误删除，再宣布升级完成。

每次发布说明 wire、client surface 是否变化，以及是否涉及持久化布局/语义变化。wire 改变本身不要求 stopworld；涉及持久化变化时，先完成该次改动的升级分析，再决定允许的替换方式与停写范围。Hello 只保护 RPC，不保护磁盘数据或共享 etcd 状态。

### 6.4 缩小中断范围与时间

当前约束下，缩小中断首先依赖部署准备和实际依赖拓扑。全局 wire 精确匹配不提供新旧服务间的桥接；共享 manager、跨分区共享的 EN 会扩大影响范围，不能仅按进程数量推断受影响分区数量。当前 stream append 等待全部副本成功，不能按“保留多数副本即可继续写”的假设安排升级。

发布前完成 binary/image 分发、配置与数据兼容检查，并记录 partition → PS → 活跃 stream 的 EN 副本依赖、已封存数据的读取依赖和 manager 依赖。明确每批会影响哪些分区的读、写和控制请求；节点被多个分区共享时取实际影响集合，不能承诺逐分区隔离。

wire 改变时，在验证 drain、ownership 和启动条件后，协调切换紧密相关的 PS/EN，使用有上限的并发缩短依赖版本不一致的时间。不能把整个发布做成每个实例等待完整业务 Ready 的长串行流程，也不能单靠提高并发保证业务正确。准备阶段不让新旧进程同时写同一数据目录或持有同一 partition ownership。

manager 领导权切换单独安排：先完成准备，再在协调的切换窗口更换 leader，避免新 manager 提前当选导致旧 wire 节点大面积失联。现有 etcd 自动选举没有升级版本门槛；若要提前启动新 manager standby，需补充并验证禁止提前竞选的控制，不能假定启动后自然保持 standby。旧 leader 也不能为已升级节点提供跨 wire 控制服务。

升级前确定有界的维护范围，避免预期心跳中断触发无谓的 reassignment、recovery、rebalance 或 GC，延长业务恢复。现有 EN maintenance 与 PS 心跳判死分别核查，不把 EN maintenance 当作 PS 的升级保护；保留 ownership fencing 和维护范围外的真实故障处理，超时后明确恢复策略。

版本拒绝须快速返回，与请求超时分开。读取路径在符合现有 committed-prefix/EC 规则时尽快选择可用副本，避免对每个拒绝端点耗尽完整 timeout；后台连接退避不能直接成为前台请求的长等待。一条业务路径恢复后即可恢复其服务，无需人为等待全部无关节点更新，但共享依赖可能使多个分区一起受阻。

验收分别测量不可读分区数、不可写分区数、最长连续中断、首个分区恢复时间和全量恢复时间。若要求 wire 改变时也严格限制为单个分区受影响，需要另行解决跨版本共享 manager 与 EN 的服务隔离；当前部署优化不能单独提供这个保证。

## 7. 持久化变更按次处理

项目持久化格式改动很少，每次涉及改动的升级单独处理。本计划只约定变更时需要说明的事项，不建设统一 read/write 入口、通用版本化 codec、全格式 envelope 或持久化防绕过框架。不以全局 cluster_version 数值证明数据兼容。

### 7.1 触发条件与分析范围

发布没有持久化布局或语义变化时，明确记录“无 persist 变化”，不要求为 Hello 改造重构现有存储路径、补齐格式版本或安排转换。

涉及 persist 变化时，仅分析受影响格式和相关路径。按本次改动需要决定是否使用现有格式版本、增加局部标识、保留历史 reader 或执行一次性转换，不预设统一机制。格式编号本身不能证明语义兼容。

分析范围可包括受影响的 manager etcd records、SST/entry、WAL/value pointer、checkpoint/TableLocations、EN metadata、FS 元数据或 control records。存在关联的格式一起分析，并核查恢复、GC、compaction、repair、FUSE/SDK 等实际读写方；这不是要求本次发布完成全项目持久化目录改造。

应用 opaque value 不属于 Autumn schema；eversion/ownership generation 不等于格式版本。

### 7.2 单次变更说明

1. 说明具体改变的布局或语义、受影响的 reader/writer，以及新旧 build 对相关数据的实际读写兼容性。
2. 决定本次能否新旧进程共同运行；若不能，明确需要停止的 writer、drain/fencing 范围及是否 stopworld，不能依赖 Hello retry。
3. 如需转换，给出本次 converter、执行顺序、服务启动/停止条件和必要资源；不需要转换时说明依据。
4. 按受影响格式验证历史数据读取与恢复、转换结果及相关引用一致性。涉及 offset/VP/checkpoint/ownership 时验证对应关系；有新旧格式识别时，检查失败不能被误当作坏尾部或缺失数据而继续覆盖状态。
5. 明确失败后的接续或恢复方案，以及是否允许直接回滚 binary；需要备份或反向转换时写出具体步骤。
6. 将该次可执行升级与验收步骤写入 docs/ops.md 或本次迁移文档。

## 8. 防遗漏与性能

- 统一连接类型、RPC 分派与 codec 的模块可见性和 CI 架构检查约束绕过 Hello；审计直接使用 transport、rpc_oneshot、bulk、admin 和跨 shard 路径。
- 固定字节测试覆盖 bootstrap 请求/成功/拒绝/外层 framing。bootstrap 格式变化必须走独立迁移决策，不能只更新测试期望值。持久化测试随具体改动安排，验证受影响历史数据、转换和恢复，不要求建立统一布局测试框架。
- RPC 业务布局变更的 wire bump 由样本测试和 review 共同约束，不恢复全文件源码 fingerprint。编码不变但语义变化无法全部自动识别，review 检查升级分析和验收证据。
- RPC 版本比较合并到 Hello，不引入每请求版本查询或全局锁。持久化检查沿用现有路径，需要调整时随具体格式改动处理。
- 沿用 rkyv 性能路径。验证正常连接只握手一次，池复用不增加握手 RTT，client → EN direct-read 的带宽/延迟及 PS 负载无意外退化。

## 9. 实施阶段与退出条件

阶段按依赖推进。首次 stopworld 部署必须等待前四阶段完成；后续同 wire 与 wire 改变的 rolling upgrade 分别验收。wire 改变不新增全局停机要求，持久化限制按具体升级单独处理。

| 阶段 | 交付 | 退出条件 |
|---|---|---|
| 1. 路径与协议审计 | 连接路径表、opcode 准入矩阵、精确 bootstrap 字节规范、现有客户端兼容区间 | 所有生产路径归类；peer/admin/client 的构造入口与冻结范围明确 |
| 2. Hello 与请求准入 | 已校验连接类型、listener 状态机、各连接池及 SDK/工具改造 | 无 Hello、malformed、wire mismatch、错误角色均在业务解码前拒绝；覆盖 direct-read 和重连 |
| 3. 启动与恢复 | 等待/Ready 状态、错误分类、mismatch 退避重连、绝对 deadline、写重试规则 | mismatch 业务拒绝且等待可观测；节点等待不被有限 retry 意外终止；超时/取消请求不继续重放 |
| 4. 首次发布准备 | 旧 gate 依赖审计及退役、ops stopworld 清单、全调用方 build 清单；说明是否涉及 persist 变化 | 若涉及 persist 变化，该次迁移与恢复验收通过；实际支持集合与现有客户端区间一致；首次上线及失败恢复演练通过 |
| 5. 首次部署 | 按第 6.1 节执行 stopworld | 全部服务端/调用方已更新、旧连接关闭、注册与业务恢复后才开放流量 |
| 6. 后续升级流程 | 同 wire 不同 build 与不同 wire 的真实 rolling 演练、异 wire RPC 拒绝测试 | 同 wire Ready 后推进；wire 改变可在预期等待状态下推进；manager failover、升级超时、最终业务恢复及 ACK 数据验证通过 |

首次发布不安排全格式 envelope 补齐或统一持久化入口建设。旧 gate 的实际依赖在阶段 4 闭环；若本次确实改变 persist 格式，再单独完成第 7 节的分析与处理。后续每次 persist 改动重新制定升级步骤，不能沿用第一次 stopworld 的结论。

### 9.1 必需验收场景

1. Hello 成功/拒绝固定字节、未知 bootstrap 版本、非法长度、超时、无 Hello、错误服务类型和重复 Hello。
2. 内部/admin wire 两方向 mismatch 均没有业务 body 被 rkyv 解码；client 冒用内部/admin opcode 在 body 解码前拒绝。
3. EN 注册使用 peer、autumn-op 使用 admin；所有 ConnPool、rpc_oneshot、节点直连、跨 shard、bulk、streaming 和后台路径完成握手。
4. SDK → EN direct-read 在支持的客户端版本下成功；EN 重启、换 replica、连接池替换后重新 Hello；同地址不同角色的连接不混用。
5. 首次 stopworld 使用真实未更新握手的 SDK/工具验证遗留调用方被拒绝，使用更新后的 SDK 验证 direct-read；支持统一 Hello 的真实客户端验证现有区间的下界、内部及上界可用，区间外被拒绝，不靠伪造版本整数证明兼容。
6. 异 wire 注册/业务 RPC 被拒绝，不能因 retry 或 best-effort skip 获得连接准入；节点可持续处于可观测等待，部署器可按既定条件推进后续实例；重启重连重新 Hello；单次请求在自己的 deadline 内结束，取消后不再重试。
7. 已执行但响应丢失的写按各入口幂等/去重规则处理；无去重能力的操作不盲目重放；恢复后验证已 ACK 数据与 ownership。
8. 首次 Hello stopworld、后续不同 wire 的 rolling upgrade 与同 wire rolling upgrade 分别演练。不同 wire 演练使用两个都支持统一 Hello 的真实 build，覆盖两个版本切换方向、manager failover、未 Ready 时推进、心跳中断后的控制任务和最终恢复；报告请求失败、超时、结果未知及业务中断范围，验证已 ACK 的持久化写不丢失。

若本次涉及 persist 变化，另按该次变更说明验收受影响数据的兼容性、转换、恢复和回滚；这些验收不作为无 persist 变化的 Hello 发布的额外改造任务。

RPC 显式准入与 cluster_version/cluster bump 删除已实现。旧一次性 SST/FS 转换工具和旧持久化开关注释已删除；现有 SST/FS 格式拒绝检查保留。未执行部署、etcd 遗留 key 清理或历史数据转换。上述发布演练与真实历史客户端兼容性验收仍由具体发布完成，不能以握手测试中声明的版本整数代替。
