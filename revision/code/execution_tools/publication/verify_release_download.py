"""Download only the two public Release assets and verify exact final bytes."""
from pathlib import Path
import hashlib,json,urllib.request
P=Path(__file__).resolve().parent;R=P.parent
OUT=R.parents[3]/'04_deliverables/stage1C/revisions'/R.name

def main():
    release=json.loads((P/'RELEASE_METADATA.json').read_text(encoding='utf-8'))
    expected={'Stage1C_R1_Public_Review_Bundle.zip','Stage1C_R1_Public_Review_Bundle.zip.sha256'}
    assert {a['name'] for a in release['assets']}==expected
    dest=P/'remote_download';dest.mkdir(exist_ok=False);rows=[]
    for asset in release['assets']:
        name=asset['name'];url=asset['url']
        assert url.startswith('https://api.github.com/repos/fakerealmike6-netizen/TheSecondPaper/releases/assets/')
        local=(OUT/name).read_bytes();assert asset['size']==len(local)
        request=urllib.request.Request(url,headers={'User-Agent':'TheSecondPaper-Stage1C-R1-public-verification','Accept':'application/octet-stream'})
        with urllib.request.urlopen(request,timeout=60) as response:
            data=response.read(len(local)+1)
            assert len(data)==len(local) and not response.read(1),'Wrong asset length'
        (dest/name).write_bytes(data)
        assert data==local,'Remote bytes differ: '+name
        digest=hashlib.sha256(data).hexdigest()
        assert asset.get('digest')=='sha256:'+digest
        rows.append({'filename':name,'bytes':len(data),'sha256':digest,'asset_id':asset['id'],'asset_api_url':url,'matches_local_byte_for_byte':True})
    receipt={'passed':True,'public_assets_only':True,'download_route':'Anonymous public asset API with application/octet-stream','assets':rows}
    (P/'REMOTE_DOWNLOAD_VERIFICATION.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(receipt))

if __name__=='__main__':main()
