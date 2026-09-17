import os, asyncio, time, json
from collections import deque
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx, websockets

app=FastAPI(title='Klinger BTC Server')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
FAST,SLOW,SIG=34,55,13
state={'running':False,'end_at':0,'last_signal':None,'kvo':None,'signal':None,'price':None,'last_error':None}
task=None
bars=[]

def ema(values,n):
    out=[None]*len(values)
    if len(values)<n:return out
    v=sum(values[:n])/n; out[n-1]=v; a=2/(n+1)
    for i in range(n,len(values)):
        v=a*values[i]+(1-a)*v; out[i]=v
    return out

def klinger_full(bs):
    # Klinger clássico: tendência por H+L+C, DM=H-L, CM acumulado por tendência,
    # VF=V*abs(2*(DM/CM-1))*trend*100; KVO=EMA34(VF)-EMA55(VF); sinal=EMA13(KVO)
    if len(bs)<100:return None
    vf=[]; prev_trend=1; prev_dm=0.0; prev_cm=0.0; prev_hlc=None
    for i,b in enumerate(bs):
        hlc=b['h']+b['l']+b['c']; dm=b['h']-b['l']
        if prev_hlc is None: trend=1
        elif hlc>prev_hlc: trend=1
        elif hlc<prev_hlc: trend=-1
        else: trend=prev_trend
        cm=(prev_cm+dm) if trend==prev_trend else (prev_dm+dm)
        val=b['v']*abs(2*((dm/cm)-1))*trend*100 if cm else 0.0
        vf.append(val); prev_trend,prev_dm,prev_cm,prev_hlc=trend,dm,cm,hlc
    ef,es=ema(vf,FAST),ema(vf,SLOW); k=[None]*len(bs)
    for i in range(len(bs)):
        if ef[i] is not None and es[i] is not None:k[i]=ef[i]-es[i]
    first=next((i for i,x in enumerate(k) if x is not None),None)
    if first is None:return None
    compact=[x for x in k[first:] if x is not None]; se=ema(compact,SIG); sg=[None]*len(bs)
    for j,x in enumerate(se): sg[first+j]=x
    return k,sg

def candle_color(b): return 'green' if b['c']>b['o'] else 'red' if b['c']<b['o'] else 'doji'

async def telegram(text):
    token=os.getenv('TELEGRAM_BOT_TOKEN'); chat=os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat:return
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(f'https://api.telegram.org/bot{token}/sendMessage',data={'chat_id':chat,'text':text})

async def seed():
    global bars
    async with httpx.AsyncClient(timeout=15) as c:
        r=await c.get('https://api.binance.com/api/v3/klines',params={'symbol':'BTCUSDT','interval':'1m','limit':1000});r.raise_for_status()
    bars=[{'t':int(x[0]),'o':float(x[1]),'h':float(x[2]),'l':float(x[3]),'c':float(x[4]),'v':float(x[5]),'closed':True} for x in r.json()]

async def monitor():
    global bars
    try:
        await seed(); last_signal_bar=None; pending=None
        async with websockets.connect('wss://stream.binance.com:9443/ws/btcusdt@kline_1m',ping_interval=20,ping_timeout=20) as ws:
            while state['running'] and time.time()<state['end_at']:
                raw=await asyncio.wait_for(ws.recv(),timeout=30); m=json.loads(raw); q=m['k']
                b={'t':int(q['t']),'o':float(q['o']),'h':float(q['h']),'l':float(q['l']),'c':float(q['c']),'v':float(q['v']),'closed':bool(q['x'])}
                state['price']=b['c']
                if bars and bars[-1]['t']==b['t']: bars[-1]=b
                else: bars.append(b); bars=bars[-1000:]
                ks=klinger_full(bars)
                if not ks: continue
                k,sg=ks; n=len(bars)-1; p=n-1
                state['kvo']=k[n]; state['signal']=sg[n]
                left=max(0,60-(int(time.time())%60))
                if left<=30 and last_signal_bar!=b['t'] and all(x is not None for x in (k[p],sg[p],k[n],sg[n])):
                    up=k[p]<=sg[p] and k[n]>sg[n] and k[n]>k[p]
                    down=k[p]>=sg[p] and k[n]<sg[n] and k[n]<k[p]
                    typ='COMPRA' if up and candle_color(b)=='green' else 'VENDA' if down and candle_color(b)=='red' else None
                    if typ:
                        last_signal_bar=b['t']; pending={'type':typ,'bar':b['t']}; state['last_signal']={'type':typ,'time':int(time.time()),'price':b['c']}
                        await telegram(f'🟡 {typ} — BTCUSDT 1m\nPreço: {b["c"]:.2f}\nAproximadamente {left}s para fechar a vela.')
                if b['closed'] and pending and pending['bar']!=b['t']:
                    col=candle_color(b)
                    if col!='doji':
                        ok=(pending['type']=='COMPRA' and col=='green') or (pending['type']=='VENDA' and col=='red')
                        await telegram(('🟢 LUCRO' if ok else '🔴 LOS')+' — resultado da próxima vela.')
                        pending=None
    except Exception as e: state['last_error']=str(e)
    finally:
        state['running']=False

@app.get('/')
def root(): return {'ok':True,'service':'Klinger BTC','running':state['running']}
@app.get('/status')
def status(): return state
@app.post('/start')
async def start():
    global task
    if not state['running']:
        state.update(running=True,end_at=time.time()+1800,last_error=None)
        task=asyncio.create_task(monitor())
    return state
@app.post('/stop')
async def stop():
    state['running']=False
    return state
