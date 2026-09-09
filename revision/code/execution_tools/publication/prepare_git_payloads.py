"""Build exact public-only Git payloads from the final verified public tree.

No credentials or network access. Content batches only create objects; the
final tree uses existing blob identities after all batches have succeeded.
"""
from pathlib import Path
import hashlib,json,sys

P=Path(__file__).resolve().parent; R=P.parent
sys.path.insert(0,str(R/'code/src'))
from archive_safety import verify_manifest
from run_stage1c import read,write,file_hash

def blob(data):
    return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()

def main():
    tree=R/'prepared_final/public'
    assert read(R/'prepared_final/PUBLIC_CONTENT_SCAN.json')==[]
    assert read(R/'prepared_final/PREPARED_TREES.json')['final']
    verify_manifest(tree/'12_FILE_HASHES.txt')
    original=read(P/'BASELINE_PUBLIC_TREE.json')
    assert original['sha']=='cef2f09aaa6033e9cb7393d6dc5cad6119a4f450' and not original['truncated']
    base={e['path']:e for e in original['tree'] if e['type']=='blob'}
    known={e['sha'] for e in base.values()}
    # Immutable objects whose creation already succeeded may be reused only if
    # their exact content hash matches a file in the newly scanned public tree.
    prior_proof=P/'REJECTED_CANDIDATE_REMOTE_OBJECT_CHECK.json'
    if prior_proof.exists():
        for row in read(prior_proof)['successfully_created_unreferenced_trees']:
            prior=read(P/'rejected_candidate_payloads'/f"batch_{row['batch']:03d}.json")
            for entry in prior['tree_elements']:
                known.add(blob(entry['content'].encode('utf-8')))
    current={p.relative_to(tree).as_posix():p for p in sorted(tree.rglob('*')) if p.is_file()}
    final=[];pending=[];seen=set();inventory=[]
    for name,p in current.items():
        data=p.read_bytes();sha=blob(data)
        final.append({'path':name,'mode':'100644','type':'blob','sha':sha})
        inventory.append({'path':name,'bytes':len(data),'blob_sha1':sha,'sha256':file_hash(p)})
        if sha in known or sha in seen:continue
        content=data.decode('utf-8')
        assert content.encode('utf-8')==data
        seen.add(sha)
        pending.append({'path':name,'mode':'100644','type':'blob','content':content})
    removed=sorted(set(base)-set(current))
    final.extend({'path':n,'mode':'100644','type':'blob','sha':None} for n in removed)
    destination=P/'payloads';destination.mkdir(exist_ok=False)
    batches=[];chunk=[];size=0
    for item in pending:
        weight=len(json.dumps(item,ensure_ascii=False).encode('utf-8'))
        if chunk and size+weight>400_000:
            batches.append(chunk);chunk=[];size=0
        chunk.append(item);size+=weight
    if chunk:batches.append(chunk)
    for i,items in enumerate(batches):
        write(destination/f'batch_{i:03d}.json',{'repository_full_name':'fakerealmike6-netizen/TheSecondPaper','base_tree_sha':original['sha'],'tree_elements':items})
    write(destination/'final_tree.json',{'repository_full_name':'fakerealmike6-netizen/TheSecondPaper','base_tree_sha':original['sha'],'tree_elements':final})
    summary={'public_files':len(current),'new_unique_blobs':len(pending),'reused_existing_blobs':len(current)-len(pending),
             'content_batches':len(batches),'removed_parent_paths':removed,'expected_tree':read(R/'checks/ARCHIVE_IDENTITIES.json')['public']['public_git_tree_sha1'],
             'publication_files':inventory,'public_only':True,'no_ref_mutation_by_this_script':True}
    write(P/'PUBLICATION_PAYLOAD_INVENTORY.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('publication_files','removed_parent_paths')}))

if __name__=='__main__':main()
