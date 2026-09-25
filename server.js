import express from "express";
import dotenv from "dotenv";
import crypto from "crypto";
import axios from "axios";
import { WebSocketServer, WebSocket } from "ws";

dotenv.config();
const app=express();
const PORT=Number(process.env.PORT||3000);
const CLIENT_ID=process.env.FYERS_CLIENT_ID||"";
const SECRET=process.env.FYERS_SECRET_KEY||"";
const REDIRECT=process.env.FYERS_REDIRECT_URI||`http://localhost:${PORT}/callback`;
const BASE=process.env.BASE_URL||`http://localhost:${PORT}`;
const SYMBOLS=(process.env.SYMBOLS||"NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX").split(",").map(s=>s.trim()).filter(Boolean);

let accessToken="";
let fyersWS=null;
let reconnectTimer=null;
const clients=new Set();
const market={};
const candles={};
const lastCandle={};

app.use(express.static("public"));
app.get("/health",(req,res)=>res.json({ok:true,connected:Boolean(accessToken&&fyersWS&&fyersWS.readyState===WebSocket.OPEN),symbols:SYMBOLS}));
app.get("/login",(req,res)=>{
  if(!CLIENT_ID||!SECRET) return res.status(500).send("FYERS credentials are not configured on the server.");
  const state=crypto.randomBytes(16).toString("hex");
  const u=new URL("https://api-t1.fyers.in/api/v3/generate-authcode");
  u.searchParams.set("client_id",CLIENT_ID);
  u.searchParams.set("redirect_uri",REDIRECT);
  u.searchParams.set("response_type","code");
  u.searchParams.set("state",state);
  res.redirect(u.toString());
});
app.get("/callback",async(req,res)=>{
  const code=req.query.auth_code||req.query.code;
  if(!code) return res.status(400).send("No authorization code was returned by FYERS.");
  try{
    const hash=crypto.createHash("sha256").update(CLIENT_ID+SECRET).digest("hex");
    const r=await axios.post("https://api-t1.fyers.in/api/v3/token",
      {grant_type:"authorization_code",appIdHash:hash,code},
      {headers:{"Content-Type":"application/json"}});
    if(!r.data?.access_token) throw new Error(JSON.stringify(r.data));
    accessToken=r.data.access_token;
    startFyersSocket();
    res.redirect("/");
  }catch(e){
    console.error("TOKEN ERROR",e.response?.data||e.message);
    res.status(500).send("FYERS token exchange failed. Check App ID, Secret, redirect URI and app activation.");
  }
});

function broadcast(obj){
  const msg=JSON.stringify(obj);
  for(const c of clients) if(c.readyState===WebSocket.OPEN) c.send(msg);
}
function broadcastStatus(){
  broadcast({type:"status",connected:Boolean(fyersWS&&fyersWS.readyState===WebSocket.OPEN)});
}
function startFyersSocket(){
  if(fyersWS) try{fyersWS.close()}catch{}
  clearTimeout(reconnectTimer);
  fyersWS=new WebSocket("wss://api.fyers.in/socket/v2/data/");
  fyersWS.on("open",()=>{
    console.log("FYERS market socket connected");
    // FYERS API v3 market-data socket expects the access token in the authentication message.
    fyersWS.send(JSON.stringify({authorization:`${CLIENT_ID}:${accessToken}`,action:1,data:{}}));
    setTimeout(()=>{
      if(fyersWS?.readyState===WebSocket.OPEN){
        fyersWS.send(JSON.stringify({symbol:SYMBOLS,type:"symbolUpdate"}));
      }
    },300);
    broadcastStatus();
  });
  fyersWS.on("message",buf=>{
    let d; try{d=JSON.parse(buf.toString())}catch{return}
    if(d.type==="sf"||d.s==="error"){ broadcast({type:"error",message:d.message||"FYERS stream error"}); return; }
    const symbol=d.symbol||d.sym||d.n;
    const ltp=Number(d.ltp??d.price);
    if(!symbol||!Number.isFinite(ltp)) return;
    const volume=Number(d.vol??d.volume??0);
    const ts=Number(d.timestamp??d.ft??Date.now());
    const item=market[symbol]||{};
    market[symbol]={...item,ltp,volume,timestamp:ts};
    updateCandle(symbol,ltp,volume,ts);
    broadcast({type:"tick",symbol,ltp,volume,timestamp:ts,candle:candles[symbol]?.at(-1)||null});
  });
  fyersWS.on("close",()=>{broadcastStatus(); if(accessToken) reconnectTimer=setTimeout(startFyersSocket,3000)});
  fyersWS.on("error",e=>{console.error("WS",e.message); broadcast({type:"error",message:"FYERS WebSocket error; reconnecting…"});});
}
function updateCandle(symbol,price,cumVol,ts){
  const minute=Math.floor(ts/60000)*60000;
  let c=lastCandle[symbol];
  if(!c||c.t!==minute){
    if(c){ candles[symbol]=(candles[symbol]||[]).concat(c).slice(-500); }
    const prevVol=c?Math.max(0,cumVol-c.vcum):0;
    c={t:minute,o:price,h:price,l:price,c:price,v:prevVol,vcum:cumVol};
    lastCandle[symbol]=c;
  }else{
    c.h=Math.max(c.h,price); c.l=Math.min(c.l,price); c.c=price;
    c.v=Math.max(0,cumVol-c.vcum);
    c.vcum=cumVol;
  }
}
const wss=new WebSocketServer({server:app.listen(PORT,()=>console.log(`Open ${BASE}`))});
wss.on("connection",ws=>{
  clients.add(ws);
  ws.send(JSON.stringify({type:"status",connected:Boolean(fyersWS&&fyersWS.readyState===WebSocket.OPEN)}));
  ws.on("close",()=>clients.delete(ws));
});
