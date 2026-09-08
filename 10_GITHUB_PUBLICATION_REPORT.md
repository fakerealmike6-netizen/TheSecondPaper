# Publication and immutable identity

The public tree appends to baseline commit `81eff7601acfc15ce5dfdabe5fd328073499de09`. Only a unique Stage1C branch/tag/Release is permitted; old main/tags are not updated or force-pushed.

The installed GitHub CLI rejected its credential during preparation. The existing authorized GitHub connector can transfer public Git objects. A narrowly scoped publication workflow, triggered only on the unique Stage1C branch, uses the repository temporary contents-write token to build a deterministic public ZIP and create the requested Release. It reads only the public commit and does not run research queries or experiments. Linux packaging, if executed there, is not claimed as Linux experiment validation. The external publication receipt records which steps actually succeeded.

The public ZIP uses fixed timestamps, permissions and ZIP_STORED, so GitHub packaging can reproduce its bytes independently of the compressor version. Private MIN and handoff are never uploaded. Public payload inventory is bound by PUBLIC_LOCAL_EQUIVALENCE.json; the file manifest binds payload and equivalence; ZIP hashes are external sidecars. Commit and publication metadata are external to the committed public payload, avoiding self-hash cycles.
