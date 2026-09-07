# Stage1B-R2 checkpoint

Run `20260907T105844+0800_stage1b_r2` continues `20260906T212828+0800_stage1b_r1`; the original MIN SHA-256 is `e5fe7d3d6727503862e3faa616ca239bef1398c594f40d64909329899eb0a64f`. The supplied continuation package was 300048 bytes, SHA-256 `bfad8ea13a7a3a316fd25ceb0b4071484b7f4ffac4cb47f735a918863d7962be`; CRC, safe extraction and all outer/inner manifest entries passed.

The three directed repairs passed the continuation gate, followed automatically by real collection. Nine new SQL executions and nine fully exported pages produced 150 rows: 120 indexed-event rows, 25 address-result rows and five WETH trace rows. Atomic added one physical event and one candidate; Harmony added 52 physical events and 46 candidates. Both declared acquisition scopes completed with zero unresolved frontiers. No full-sample run or next stage began.

Atomic has 12 candidates and six unique target entry events; its conditional address-ETH joint interval is [0,359.495] ETH. Harmony has 50 candidates, eight unique target entry events and seven address-ETH groups. The new targets arise from real observed events and newly matched labels. Financial lower bounds are conditional model results, not observed zero balances.

Scientific completeness remains PARTIAL: initial balances and complete gas attribution are missing, and the real WETH conversion remains NOT_CERTIFIED. External acceptance remains PENDING_REVIEW. The stopped worker has no unresolved new submit/export attempt; legacy cost risk remains preserved.

Known actual fees are at least 21.005321989 credits. Adding 10.000000000 documented export upper occupancy and 8.582436627 legacy unknown risk gives 39.587758616 of the authorized 100-credit cumulative ceiling.
