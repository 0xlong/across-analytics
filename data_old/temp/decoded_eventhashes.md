## Event Signatures - Arbitrum SpokePool (Across Protocol)

| Full Signature | Topic Hash | Category |
|----------------|------------|----------|
| `FilledV3Relay(address,address,uint256,uint256,uint256,uint256,uint32,uint32,uint32,address,address,address,address,bytes,bytes)` | `0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7` | Relay (V3) |
| `V3FundsDeposited(address,address,uint256,uint256,uint256,uint32,uint32,uint32,uint32,address,address,address,bytes)` | `0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f` | Deposit (V3) |
| `FilledRelay(bytes32,bytes32,uint256,uint256,uint256,uint64,uint64,uint64,address,address,address,address,bytes,bytes)` | `0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208` | Relay (v2) |
| `FundsDeposited(bytes32,bytes32,uint256,uint256,uint256,uint64,uint32,uint32,address,address,address,bytes)` | `0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3` | Deposit (v2) |
| `FilledRelay(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint32,uint32,address,address,address,address,bytes,bytes,bytes)` | `0x8ab9dc6c19fe88e69bc70221b339c84332752fdd49591b7c51e66bae3947b73c` | Relay (v1) |
| `FundsDeposited(uint256,uint256,uint256,int64,uint32,uint32,address,address,address,bytes)` | `0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6` | Deposit (v1) |
| `ExecutedRelayerRefundRoot(uint256,uint256,uint256[],bytes32,address,address[])` | `0xf8bd640004bcec1b89657020f561d0b070cbdf662d0b158db9dccb0a8301bfab` | Refund (v1) |
| `ExecutedRelayerRefundRoot(uint256,uint256,uint256[],uint32,uint32,bytes32,address,address[])` | `0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e` | Refund (v2) |
| `RelayedRootBundle(uint32,bytes32,bytes32)` | `0xc86ba04c55bc5eb2f2876b91c438849a296dbec7b08751c3074d92e04f0a77af` | Bundle |
| `RequestedV3SlowFill(address,address,uint256,uint256,uint256,uint32,uint32,uint32,uint32,address,bytes)` | `0x923794976d026d6b119735adc163cb71decfc903e17c3dc226c00789593c04e1` | SlowFill (V3) |
| `TokensBridged(uint256,uint256,uint32,address,address)` | `0x828fc203220356df8f072a91681caee7d5c75095e2a95e80ed5a14b384697f71` | Bridge (v1) |
| `ArbitrumTokensBridged(address,address,uint256)` | `0x997d81a0a8415d688a6c319736602098252bf6445e0e879326f682f11928e317` | Bridge (Arb) |
| `RequestedSlowFill(bytes32,bytes32,uint256,uint256,uint256,uint64,uint64,uint32,uint32,address,address,address,bytes)` | `0x3cee3e290f36226751cd0b3321b213890fe9c768e922f267fa6111836ce05c32` | SlowFill (Legacy) |
| `TokensBridged(uint256,uint256,uint32,bytes32,address,address)` | `0xfa7fa7cf6d7dde5f9be65a67e6a1a747e7aa864dcd2d793353c722d80fbbb357` | Bridge (v2) |
| `RequestedSpeedUpDeposit(int64,uint32,address,address,bytes,bytes)` | `0xa6aa57bd282fc82378458de27be4eadfa791a0c7321c49562eeba8b2643dd566` | SpeedUp |
| `EnabledDepositRoute(address,uint256,bool)` | `0x0a21fdd43d0ad0c62689ee7230a47309a050755bcc52eba00310add65297692a` | Admin |
| `RequestedSpeedUpV3Deposit(uint256,uint32,address,address,bytes,bytes)` | `0xb0a29aed3d389a1041194255878b423f7780be3ed2324d4693508c6ff189845e` | SpeedUp (V3) |
| `WhitelistedTokens(address,address)` | `0x8d7f294eaa476236fe8cb5629376a12cd37dace3d21e6a7b98f1641c4ed5f09e` | Admin |
| `Upgraded(address)` | `0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b` | Proxy |
| `EmergencyDeleteRootBundle(uint256,bytes32,bytes32)` | `0x3569b846531b754c99cb80df3f49cd72fa6fe106aaee5ab8e0caf35a9d7ce88d` | Emergency |
| `PausedDeposits(bool)` | `0xe88463c2f254e2b070013a2dc7ee1e099f9bc00534cbdf03af551dc26ae49219` | Pause |
| `SetL2GatewayRouter(address)` | `0xdc4a5f4c066ad14c1306e624550b42395e08f992a76b416cc7b1ad11503d376c` | Admin |
| `SetHubPool(address)` | `0x1f17a88f67b0f49060a34bec1a4723a563620e6aa265eb640b5046dcee0759a0` | Admin |
| `SetXDomainAdmin(address)` | `0xa9e8c42c9e7fca7f62755189a16b2f5314d43d8fb24e91ba54e6d65f9314e849` | Admin |

---

### Event Distribution by Category

| Category | Events | % of Total |
|----------|-------:|:----------:|
| **Deposits** (V3 + Legacy) | 3,104,061 | 46.82% |
| **Relays/Fills** (V3 + Legacy) | 3,423,356 | 51.64% |
| **Refunds** | 62,917 | 0.95% |
| **SlowFills** | 9,924 | 0.15% |
| **Bundles** | 15,855 | 0.24% |
| **Token Bridges** | 7,900 | 0.12% |
| **Admin/Config** | 138 | <0.01% |
| **Other** | 17 | <0.01% |

### Protocol Version Adoption

| Version | Deposits | Fills | Notes |
|---------|----------|-------|-------|
| **V3 (Current)** | 1,644,290 | 1,792,689 | Active version |
| **V2 (Legacy)** | 1,061,217 | 1,145,619 | `bytes32` identifiers |
| **V1 (Legacy)** | 398,554 | 485,048 | `int64` timestamps |


### Migration from V2 to V3
https://docs.across.to/introduction/migration-guides/migration-from-v2-to-v3#events