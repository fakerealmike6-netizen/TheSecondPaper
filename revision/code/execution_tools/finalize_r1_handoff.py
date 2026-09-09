"""Bind external final receipts, then create one private immutable handoff ZIP."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,shutil,sys,zipfile
R=Path(__file__).resolve().parent;ROOT=R.parents[3]
OUT=ROOT/'04_deliverables/stage1C/revisions'/R.name
sys.path.insert(0,str(R/'code/src'))
from archive_safety import extract_new
from run_stage1c import read,write,file_hash

def main():
    archives=read(R/'checks/ARCHIVE_IDENTITIES.json')
    publication=read(OUT/'PUBLICATION_RECEIPT.json')
    assert publication['published'] and publication['remote_asset_verified']
    executions={}
    for kind in ('min','public'):
        receipt=read(R/'final_validation'/kind/'VALIDATION_RECEIPT.json')
        assert receipt['passed'] and receipt['status']=='PASS' and receipt['network_access_permitted'] is False and receipt['credentials_required'] is False
        assert receipt['windows_executed'] and not receipt['linux_executed']
        records={c['name']:c for c in receipt['checks']}
        assert records['unit_test_receipt']['passed']
        executions[kind]={'archive':archives[kind],'receipt_sha256':file_hash(R/'final_validation'/kind/'VALIDATION_RECEIPT.json'),'receipt':receipt}
    verification={'schema_version':'stage1c-r1-final-extracted-validation-v1','passed':True,'final_zip_hashes_bound':True,'executions':executions,
        'scientific_execution_platforms':['Windows'],'linux_archive_packaging_only':True,'external_acceptance':'PENDING_REVIEW'}
    write(OUT/'EXTRACTED_VALIDATION_RECEIPTS.json',verification)
    closure=read(R/'prepared_final/min/REPAIR_CLOSURE.json')
    closure.update(final_archives_verified=True,publication_commit=publication['commit'],external_finalization_utc=datetime.now(timezone.utc).isoformat(),
        final_extracted_validation_sha256=file_hash(OUT/'EXTRACTED_VALIDATION_RECEIPTS.json'),publication_receipt_sha256=file_hash(OUT/'PUBLICATION_RECEIPT.json'))
    write(OUT/'REPAIR_CLOSURE.json',closure)
    shutil.copyfile(R/'prepared_final/min/PUBLIC_LOCAL_EQUIVALENCE.json',OUT/'PUBLIC_LOCAL_EQUIVALENCE.json')
    commit=publication['commit'];repo='https://github.com/fakerealmike6-netizen/TheSecondPaper'
    handoff=f'''# Stage1C-R1 private review handoff

Upload Stage1C_R1_Review_Handoff.zip and its .sha256 once. It contains the final public/MIN ZIPs, sidecars and external receipts. MIN and the enclosing handoff remain private.

Internal repair completed. External acceptance **PENDING_REVIEW**. Stop **CHECKPOINT_1C_R1_REACHED**.

- Fixed public commit: {commit}
- Fixed review index: {repo}/blob/{commit}/00_REVIEW_INDEX.md
- Release: {publication['release_url']}
- Tests:950/950 (865 retained+85 new), zero skips/failures; old12/32 pass.
- Same batch:60 controlled+2 real,434 paired method outputs and124 shared scientific artifacts unchanged.
- Fault chain:four old negatives reproduced as old false successes; final32 negatives exit1,6 positives exit0. Full package fault test retains59 good+1 bad query and rejects the package with intact manifest/freeze/prerequisite tests.
- Final actual Windows extraction/replay:both archives PASS with network denied and no real credentials. Linux GitHub archive construction is packaging only.
- Research requests/cost0. No new query collection or Stage1D. Real amount truth remains unavailable; Haircut B_min is an assumption and6 legitimate NA remain.

PUBLICATION_RECEIPT.json binds commit/tree/tag and remote downloaded assets; EXTRACTED_VALIDATION_RECEIPTS.json binds actual validation to exact final ZIP hashes. PUBLIC_LOCAL_EQUIVALENCE.json maps every public payload to identical private bytes. Manifest and receipt dependencies are acyclic.
'''
    for kind in ('public','min'):
        a=archives[kind];handoff+=f"\n{a['filename']}: {a['bytes']} bytes; SHA-256 {a['sha256']}\n"
    (OUT/'REVIEW_HANDOFF.md').write_text(handoff,encoding='utf-8')
    names=[a['filename']+suffix for a in archives.values() for suffix in ('','.sha256')]
    names+=['PUBLICATION_RECEIPT.json','PUBLIC_LOCAL_EQUIVALENCE.json','EXTRACTED_VALIDATION_RECEIPTS.json','REPAIR_CLOSURE.json','REVIEW_HANDOFF.md']
    for kind in ('public','min'):
        name=kind.upper()+'_VALIDATION_RECEIPT.json';shutil.copyfile(R/'final_validation'/kind/'VALIDATION_RECEIPT.json',OUT/name);names.append(name)
    internal=''.join(file_hash(OUT/n)+'  '+n+'\n' for n in sorted(names))
    dest=OUT/'Stage1C_R1_Review_Handoff.zip'
    with zipfile.ZipFile(dest,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        for n in sorted(names)+['HANDOFF_FILE_HASHES.txt']:
            info=zipfile.ZipInfo(n,(2026,9,8,0,0,0));info.create_system=3;info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,internal.encode() if n=='HANDOFF_FILE_HASHES.txt' else (OUT/n).read_bytes(),compresslevel=9)
    digest=file_hash(dest);dest.with_name(dest.name+'.sha256').write_bytes((digest+'  '+dest.name+'\n').encode())
    extracted=R/'handoff_verified';expanded=extract_new(dest,extracted)
    for n in names:assert file_hash(extracted/n)==file_hash(OUT/n)
    completion={'stage':'Stage1C-R1','checkpoint':'CHECKPOINT_1C_R1_REACHED','external_acceptance':'PENDING_REVIEW',
        'archives':archives|{'handoff':{'filename':dest.name,'bytes':dest.stat().st_size,'sha256':digest,'expanded_bytes':expanded}},
        'commit':commit,'public_tree':publication['tree'],'release':publication['release_url'],'all_final_archives_extracted_and_verified':True,
        'science_tests_on':['Windows'],'research_platform_requests':0,'new_research_cost':'0','Stage1D_started':False}
    write(R/'RUN_COMPLETION.json',completion);write(OUT/'DELIVERY_INDEX.json',completion)
    print(json.dumps(completion,indent=2))
if __name__=='__main__':main()
