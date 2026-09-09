"""Create an external publication receipt from actual API/download evidence."""
from pathlib import Path
from datetime import datetime,timezone
import json,sys
P=Path(__file__).resolve().parent;R=P.parent
sys.path.insert(0,str(R/'code/src'))
from run_stage1c import read,write,file_hash
OUT=R.parents[3]/'04_deliverables/stage1C/revisions'/R.name

def main():
    expected_tag='stage1c-r1-20260908T104446-0800'
    commit=read(P/'CREATED_COMMIT.json');release=read(P/'RELEASE_METADATA.json')
    refs=read(P/'REFS_AFTER.json');before=read(P/'GITHUB_PREFLIGHT.json')['refs_before']
    download=read(P/'REMOTE_DOWNLOAD_VERIFICATION.json')
    run=read(P/'WORKFLOW_RUN.json');archives=read(R/'checks/ARCHIVE_IDENTITIES.json')
    assert commit['tree']['sha']==archives['public']['public_git_tree_sha1']
    assert [p['sha'] for p in commit['parents']]==['d88b839361b8fa93308430641baf7af423d76ae8']
    assert release['tag_name']==expected_tag and not release['draft'] and not release['prerelease']
    assert run['head_sha']==commit['sha'] and run['conclusion']=='success' and run['status']=='completed'
    by_ref={r['ref']:r['object'] for r in refs}
    assert len(by_ref)==len(refs)
    retained=[]
    for prior in before:
        current=by_ref[prior['ref']]
        assert current['sha']==prior['object']['sha'] and current['type']==prior['object']['type']
        retained.append({'ref':prior['ref'],'sha':current['sha'],'unchanged':True})
    for category in ('heads','tags'):
        identity=by_ref[f'refs/{category}/{expected_tag}']
        assert identity['type']=='commit' and identity['sha']==commit['sha']
    assert download['passed'] and len(download['assets'])==2
    public=next(a for a in download['assets'] if a['filename'].endswith('.zip'))
    assert public['sha256']==archives['public']['sha256'] and public['bytes']==archives['public']['bytes']
    receipt={'schema_version':'stage1c-r1-publication-receipt-v1','published':True,'remote_asset_verified':True,
        'repository':'fakerealmike6-netizen/TheSecondPaper','commit':commit['sha'],'tree':commit['tree']['sha'],
        'parent_commit':commit['parents'][0]['sha'],'branch':expected_tag,'tag':expected_tag,
        'release_url':release['html_url'],'release_id':release['id'],
        'fixed_index_url':'https://github.com/fakerealmike6-netizen/TheSecondPaper/blob/'+commit['sha']+'/00_REVIEW_INDEX.md',
        'workflow':{'id':run['id'],'url':run['html_url'],'conclusion':run['conclusion'],'head_sha':run['head_sha'],'linux_role':'Deterministic archive construction only'},
        'remote_assets':download['assets'],'old_refs_unchanged':retained,'force_push_used':False,'old_release_overwritten':False,
        'public_payload_scan_findings':read(R/'prepared_final/PUBLIC_CONTENT_SCAN.json'),
        'public_private_policy':'Only reviewed public source/tests/synthetic evidence and explicitly authorized Atomic/Harmony query aggregates. No private MIN/handoff, real identities, ledger or full third-party reference rows.',
        'user_query_aggregate_authorization':'Explicit consent retained in this conversation: 明确授权公开这些查询级汇总',
        'evidence_sha256':{n:file_hash(P/n) for n in ('CREATED_COMMIT.json','RELEASE_METADATA.json','REFS_AFTER.json','GITHUB_PREFLIGHT.json','REMOTE_DOWNLOAD_VERIFICATION.json','WORKFLOW_RUN.json')},
        'external_receipt_not_embedded_in_payload':True,'utc':datetime.now(timezone.utc).isoformat(),'external_acceptance':'PENDING_REVIEW'}
    assert receipt['public_payload_scan_findings']==[]
    write(OUT/'PUBLICATION_RECEIPT.json',receipt)
    print(json.dumps({k:receipt[k] for k in ('published','remote_asset_verified','commit','tree','tag','release_url')}))

if __name__=='__main__':main()
