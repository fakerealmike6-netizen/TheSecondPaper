"""Explicitly synthetic finite proof materials shared by controlled pipeline tests."""
import copy, hashlib
from stage1d_semantic_units import WETH, DEPOSIT_TOPIC, WITHDRAWAL_TOPIC, SYNTHETIC_SCHEMA
HOLDER='0x'+'2'*40
SPONSOR='0x'+'3'*40
SERVICE='0x'+'4'*40
OUTSIDE='0x'+'1'*40
SOURCE='''contract WETH9 {
function deposit() public payable { balanceOf[msg.sender] += msg.value; Deposit(msg.sender,msg.value); }
function() public payable { deposit(); }
function withdraw(uint wad) public { require(balanceOf[msg.sender] >= wad); balanceOf[msg.sender] -= wad; msg.sender.transfer(wad); Withdrawal(msg.sender,wad); }
}'''

def materials(kind='DEPOSIT',*,block=3,amount=6,holder=HOLDER,internal=False):
    txhash='0x'+format(block,'064x');blockhash='0x'+format(1000+block,'064x')
    path=[0] if internal else []
    policy={'chain_id':1,'contract':WETH,'tx_hash':txhash,'block_number':block,'block_hash':blockhash,
        'tx_index':1,'timestamp':block,'call_path':path,'kind':kind,'log_index':7,'holder':holder,'amount_raw':str(amount)}
    topic=DEPOSIT_TOPIC if kind=='DEPOSIT' else WITHDRAWAL_TOPIC
    log={'address':WETH,'logIndex':'0x7','transactionHash':txhash,'blockNumber':hex(block),'blockHash':blockhash,
        'transactionIndex':'0x1','removed':False,'topics':[topic,'0x'+'0'*24+holder[2:]],'data':'0x'+format(amount,'064x')}
    input_data='0xd0e30db0' if kind=='DEPOSIT' else '0x2e1a7d4d'+format(amount,'064x')
    value=amount if kind=='DEPOSIT' else 0
    frame={'type':'CALL','from':holder,'to':WETH,'input':input_data,'value':hex(value),'logs':[copy.deepcopy(log)],'calls':[]}
    if kind=='WITHDRAWAL':frame['calls']=[{'type':'CALL','from':WETH,'to':holder,'value':hex(amount),'input':'0x','calls':[]}]
    root={'type':'CALL','from':SPONSOR,'to':holder,'input':'0x1234','value':'0x0','calls':[frame]} if internal else frame
    tx={'hash':txhash,'chainId':'0x1','blockNumber':hex(block),'blockHash':blockhash,'transactionIndex':'0x1',
        'from':root['from'],'to':root['to'],'value':root['value'],'input':root['input']}
    rc={'transactionHash':txhash,'blockNumber':hex(block),'blockHash':blockhash,'transactionIndex':'0x1',
        'status':'0x1','logs':[log],'gasUsed':'0x1','effectiveGasPrice':'0x1'}
    code='0x6001600055'
    payloads={'transaction':tx,'receipt':rc,'trace':root,'historical_code':code,
        'header':{'number':hex(block),'hash':blockhash,'timestamp':hex(block)},'source_text':SOURCE,
        'source_attestation':{'reference_runtime_code':code,'source_sha256':hashlib.sha256(SOURCE.encode()).hexdigest(),
            'source_url':'https://example.invalid/controlled-source','chain_id':1,'contract':WETH}}
    context={'schema_version':SYNTHETIC_SCHEMA,'evidence_kind':'SYNTHETIC_CONTROLLED','binding_policy':{'tx_hash':txhash,'block_number':block},'payloads':payloads}
    return policy,context

def controlled_collection_inputs(*,withdraw=False,ordinary_prefix=False):
    from collector import Event
    from stage1d_semantic_units import certify_instance,context_identity,NATIVE,TOKEN
    p,c=materials();u=certify_instance(p,c);contexts={context_identity(c):c};units=[u]
    seed=Event('eip155:1:tx:0x'+'a'*64+':top','0x'+'a'*64,OUTSIDE,HOLDER,NATIVE,10,1,1,1,block_hash='0x'+'b'*64)
    native=Event(**u['native_event'])
    if withdraw:
        wp,wc=materials('WITHDRAWAL',block=4,amount=4);wu=certify_instance(wp,wc);contexts[context_identity(wc)]=wc;units.append(wu)
        entry=Event('eip155:1:tx:0x'+'c'*64+':top','0x'+'c'*64,HOLDER,SERVICE,NATIVE,3,5,1,5,block_hash='0x'+'d'*64)
    else:
        entry=Event('eip155:1:tx:0x'+'c'*64+':log:2','0x'+'c'*64,HOLDER,SERVICE,TOKEN,4,4,1,4,kind='erc20',log_index=2,block_hash='0x'+'d'*64)
    physical=[native,entry]
    if ordinary_prefix:
        upstream='0x'+'5'*40
        seed=Event(seed.event_id,seed.tx_hash,seed.sender,upstream,NATIVE,10,1,1,1,block_hash=seed.block_hash)
        physical.insert(0,Event('eip155:1:tx:0x'+'e'*64+':top','0x'+'e'*64,upstream,HOLDER,NATIVE,8,2,1,2,block_hash='0x'+'f'*64))
    return seed,physical,units,contexts
