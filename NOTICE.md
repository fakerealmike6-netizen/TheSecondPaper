# Sources, dependencies and licensing

The source in this export was authored within the current clean-room project.
General finite-reference utilities were reused from that project's accepted R1
implementation. No MFTracer or AMLGuard algorithm source is imported or bundled.
Their outputs are not amount truth. DSU-style component ideas are acknowledged
as prior work, not claimed as a new general concept.

Original-code license: awaiting the researcher's choice. No blanket MIT or other
license is asserted over original or third-party content by this publication.
Synthetic fixtures were generated for this stage. Full third-party datasets,
label stores, provider responses and account information are not redistributed.

Dependencies retain their own licenses: Python (PSF), NumPy (BSD 3-Clause),
SciPy (BSD 3-Clause), and HiGHS (MIT). Consult the installed distribution's license
and third-party notices for exact version-specific terms and bundled libraries.
No dependency binaries are included.

Provider specifications remain the providers' documentation: Etherscan API V2,
Dune Ethereum transaction/trace and ERC20 schemas, and Ethereum JSON-RPC receipt
semantics. Schema-source links are in `configs/DUNE_ADAPTER_SCHEMA.md`.
Canonical WETH9 behavior is used as a limited modeling rule; source/runtime
equivalence at a historical block requires separate evidence.
