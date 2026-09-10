"""Single-writer local operation with no online session or provider dispatch."""
from contextlib import contextmanager
from pathlib import Path
import json,os,uuid,socket

@contextmanager
def offline_operation(work,label):
    from stage1d_runtime import Runtime
    work=Path(work);Runtime().require_gate(work)
    lock=work/'private/network_worker.lock'
    payload=json.dumps({'pid':os.getpid(),'kind':'OFFLINE_REPAIR_OPERATION','label':label,
        'token':uuid.uuid4().hex,'online_clock_increment':0},sort_keys=True).encode()
    def blocked(*a,**k):raise RuntimeError('Offline repair operation cannot contact a provider')
    connect,create=socket.socket.connect,socket.create_connection
    with lock.open('xb') as handle:handle.write(payload)
    socket.socket.connect=blocked;socket.create_connection=blocked
    try:yield
    finally:
        socket.socket.connect=connect;socket.create_connection=create
        if lock.read_bytes()!=payload:raise RuntimeError('Offline lock changed; preserve for recovery')
        lock.unlink()
