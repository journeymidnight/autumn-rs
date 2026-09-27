// Execute the shipped page: failures must remain failures and policy names
// must survive the browser's HTML-attribute decoding followed by JS parsing.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const html = fs.readFileSync(process.env.DASHBOARD_HTML || path.join(__dirname, '../static/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*)<\/script>/)[1];
const ids = new Set([...html.matchAll(/\sid="([^"]+)"/g)].map(m => m[1]));
const nodes = {}, toasts = [], calls = [], windowStub = {};
const node = id => nodes[id] ||= {
  innerHTML: '', textContent: '', value: '', checked: false, style: {},
  classList: {add(){}, remove(){}}, appendChild(x){ toasts.push({...x}); },
};
const documentStub = {
  querySelector(sel){
    const id = sel.slice(1); assert(ids.has(id), `undeclared ${sel}`); return node(id);
  },
  createElement(){ return {remove(){}}; },
};
let failure = false, networkFailure = false, delayed = null;
const status = {mode:'armed', active:'custom', policies:[], log:[]};
const fetchStub = async (url, opt={}) => {
  calls.push([url, opt]);
  if(networkFailure) throw new Error('connection lost');
  if(delayed) return delayed;
  return {ok: !failure, status: failure ? 502 : 200,
    json: async () => failure ? (opt.method ? {ok:false,output:'manager refused update'} : {error:'manager unavailable'})
      : opt.method ? {ok:true,output:'done'} : status};
};
// Omit only the automatic initial poll/timer, while retaining all event wiring.
const api = new Function('document','window','location','fetch','setTimeout','setInterval','clearInterval','confirm',
  script.replace(/setTab\(TAB\);\s*schedule\(\);\s*$/, '') +
  '\nreturn {activate,stopCtl,armCtl,delPolicy,loadPolicies,renderPolicyTab,api,jsAttr,policyTab:()=>TAB="policy"};')
  (documentStub,windowStub,{hash:'#policy'},fetchStub,()=>0,()=>0,()=>{},()=>true);
(async()=>{
  api.policyTab();
  failure=true;
  node('np_name').value='my-policy'; node('np_interval').value='30'; node('np_cooldown').value='180'; node('np_max').value='2';
  await node('np_save').onclick();
  assert(toasts.some(t=>t.className==='t bad' && t.textContent.includes('manager refused update')), 'failed save must report manager refusal');
  assert(!toasts.some(t=>t.textContent.includes('Policy saved')), 'failed save must never claim success');
  assert.equal(node('np_name').value,'my-policy', 'keep failed edits');
  await api.loadPolicies();
  assert.equal(node('v_ctl').textContent,'unknown','failed status must not report Off');
  assert(node('ctlstate').textContent.includes('manager unavailable'));
  for(const fn of [()=>api.activate('custom'),()=>api.stopCtl(),()=>api.delPolicy('custom')]){
    const before=toasts.length; await fn();
    assert(toasts.slice(before).some(t=>t.className==='t bad'), 'failed mutation needs a visible error');
  }
  networkFailure=true;
  await api.stopCtl();
  assert(toasts.at(-1).textContent.includes('connection lost'));
  networkFailure=false; failure=false;
  await api.loadPolicies();
  assert.equal(windowStub.__polError,null,'successful refresh clears unknown state');
  await node('np_save').onclick();
  assert.equal(node('np_name').value,'');
  assert(toasts.some(t=>t.textContent==='Policy saved: my-policy'));

  // Names are legal manager metadata. Decode HTML entities as a browser does,
  // then execute the rendered handler with a harmless capture function.
  for(const name of ['x");globalThis.__dashboardInjected=true;//', "a'b", '&quot;);throw new Error(1);//', '<tag>\\line']){
    windowStub.__pol={...status,policies:[{name,desc:'',switches:{}}]};
    api.renderPolicyTab();
    const handlers=[...node('pollist').innerHTML.matchAll(/onclick='([^']*)'/g)].map(m=>m[1]);
    assert.equal(handlers.length,2);
    for(const handler of handlers){
      const decoded=handler.replace(/&quot;/g,'"').replace(/&#39;|&#x27;/gi,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&');
      let captured;
      new Function('activate','delPolicy',decoded)(v=>captured=v,v=>captured=v);
      assert.equal(globalThis.__dashboardInjected,undefined,'policy name must never execute script');
      assert.equal(captured,name,'rendered button must preserve exactly one policy name');
    }
  }
  let resolve;
  delayed=new Promise(r=>resolve=r);
  const before=calls.length;
  const a=api.api('/api/policies'), b=api.api('/api/policies');
  assert.equal(calls.length,before+1,'overlapping reads must share one subprocess request');
  resolve({ok:true,json:async()=>status}); await Promise.all([a,b]); delayed=null;
  console.log('policy controls OK: failures, unknown state, recovery, name escaping, overlapping reads');
})().catch(e=>{console.error(e);process.exitCode=1;});
