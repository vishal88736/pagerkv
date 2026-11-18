const {test} = require('node:test');
const assert = require('node:assert/strict');
const {dashboard, state, descendants} = require('./dashboard-harness.cjs');

test('acknowledged resident blocks appear on both isometric node floors',()=>{
  const {check,element}=dashboard();check.setStatus(state());check.render();
  const nodes=descendants(element('iso'));
  assert.equal(nodes.filter(node=>node.attributes.class?.startsWith('blk')).length,2);
  assert.ok(nodes.some(node=>node.textContent.includes('NODE A · ACTIVE CONTEXT')));
  assert.ok(nodes.some(node=>node.textContent.includes('NODE B · ARCHIVE')));
});

test('context table escapes key names',()=>{
  const {check,element}=dashboard();const data=state('<script>alert(1)</script>');
  check.renderTable(Object.entries(data.keys));
  assert.match(element('rows').innerHTML,/&lt;script&gt;/);
  assert.doesNotMatch(element('rows').innerHTML,/<script>/);
});

test('replication loops while pending and stops on acknowledgment',()=>{
  const {check,element}=dashboard();check.transfer('pending','Replicating context');
  assert.equal(element('transferPacket').animation.options.iterations,Infinity);
  check.transfer('ok','Copy acknowledged');
  assert.equal(element('transferPacket').animation.options.iterations,1);
  assert.equal(element('transfer').dataset.state,'ok');
});

test('hydration travels in reverse on both the activity strip and node connector',()=>{
  const {check,element}=dashboard();check.setStatus(state());check.transfer('hydrate','Loading full context');
  const frames=element('transferPacket').animation.frames;
  assert.equal(frames[0].transform,'translateX(368px)');
  assert.equal(frames.at(-1).transform,'translateX(0px)');
  const motion=descendants(element('iso')).find(node=>node.tag==='animateMotion');
  assert.equal(motion.attributes.keyPoints,'1;0');
});

test('reduced motion preserves text feedback without moving packets',()=>{
  const {check,element,context}=dashboard();context.matchMedia=()=>({matches:true});
  check.transfer('ok','Context safely replicated');
  assert.equal(element('transferPacket').animation,undefined);
  assert.equal(element('transferDetail').textContent,'Context safely replicated');
});

test('background recovery acknowledges the right context version',()=>{
  const {check,element}=dashboard();const prev=state(),next=state();
  prev.keys.observation.acked_version=0;prev.keys.observation.consistency_status='DIRTY_UNREPLICATED';
  check.observeReplication(prev,next);
  assert.match(element('transferDetail').textContent,/observation v1.*acknowledged/);
  assert.equal(element('transfer').dataset.state,'ok');
});

test('HTTP status failure marks the current context snapshot stale',async()=>{
  const {check,element,context}=dashboard();
  context.fetch=async()=>({ok:false,status:500,json:async()=>({})});
  await check.poll();assert.equal(element('pillA').textContent,'Offline');
  assert.match(element('comparisonChip').textContent,/unknown.*stale/);
});

test('the full local value action uses the REST key API independently of hydration',async()=>{
  const {check,element,context}=dashboard();check.setSelected('obs/key');
  let requested;
  context.fetch=async url=>{requested=url;return {ok:true,status:200,json:async()=>({value:'complete original'})};};
  await element('btnValue').onclick();
  assert.equal(requested,'/key/obs%2Fkey');assert.equal(element('detailOut').textContent,'complete original');
});


test('local HTTP acceptance and failed replica HTTP response remain separate evidence',async()=>{
  const {check,element,context}=dashboard();
  context.fetch=async()=>({ok:true,status:200,json:async()=>({version:2,replicated:false})});
  await check.writeValue('city','Raipur');
  check.renderReplicationResult({key:'city',version:2,method:'POST',path:'/replicate',http_status:503,elapsed_ms:12.7,ok:false,error:'Node B HTTP 503',source:'write',attempt:4});
  assert.match(element('requestRoute').textContent,/PUT \/key/);
  assert.match(element('requestBody').textContent,/Raipur/);
  assert.match(element('requestOutcome').textContent,/Node A: HTTP 200/);
  assert.equal(element('resultHttp').textContent,'HTTP 503');
  assert.equal(element('resultDuration').textContent,'12.7 ms');
  assert.match(element('resultReason').textContent,/503/);
});

test('replication and context badges show independent facts for the same matching copies',()=>{
  const {check,element}=dashboard();
  check.renderCopyPair({key:'obs_01',node_a:{version:3,acked_version:3,value:'complete text',residence:'PAGED_TO_NODE_B'},node_b:{version:3,value:'complete text'},comparison_status:'in_sync',node_b_state:'available'});
  assert.equal(element('pairComparison').textContent,'Replication: in sync');
  assert.equal(element('pairContext').textContent,'Context: paged stub');
  assert.equal(element('pairAVersion').textContent,'obs_01 · v3');
  assert.equal(element('pairBVersion').textContent,'obs_01 · v3');
  assert.equal(element('pairAValue').textContent,element('pairBValue').textContent);
});

test('an unavailable replica displays unknown alongside the pending update',()=>{
  const {check,element}=dashboard();
  check.renderCopyPair({key:'city',node_a:{version:2,acked_version:1,value:'Raipur',residence:'RESIDENT_FULL'},node_b:null,comparison_status:'unknown',node_b_state:'unavailable'});
  assert.equal(element('pairComparison').textContent,'Replication: unknown · 1 pending update');
  assert.match(element('pairBVersion').textContent,/unavailable/);
  assert.equal(element('pairAValue').textContent,'Raipur');
});

test('selected node labels use the actually observed replica version',async()=>{
  const {check,element,context}=dashboard();
  check.setStatus(state());check.setSelected('observation');
  context.fetch=async()=>({ok:true,status:200,json:async()=>({key:'observation',node_a:{version:1,acked_version:1,value:'primary',residence:'RESIDENT_FULL'},node_b:{version:9,value:'external'},comparison_status:'diverged',node_b_state:'available'})});
  await check.loadCopyPair();
  const labels=descendants(element('iso')).filter(node=>node.attributes.class==='block-label').map(node=>node.textContent);
  assert.ok(labels.includes('observation · v1'));
  assert.ok(labels.includes('observation · v9'));
});

test('guided demo refuses an existing failure mode without writing or changing it',async()=>{
  const {check,element,context}=dashboard();const calls=[];
  context.fetch=async(url,options)=>{calls.push([url,options.method]);return {ok:true,status:200,json:async()=>({mode:'down'})};};
  await check.demoNext();
  assert.match(element('demoProof').textContent,/Set Node B Online/);
  assert.equal(check.getDemo(),null);
  assert.equal(calls.length,1);
  assert.equal(calls[0][1],undefined);
});

test('guided demo does not advance when a write fails',async()=>{
  const {check,element,context}=dashboard();
  context.fetch=async url=>url==='/admin/node_b/fault'
    ? {ok:true,status:200,json:async()=>({mode:'none'})}
    : url.startsWith('/context/status')
      ? {ok:true,status:200,json:async()=>({max_window_tokens:2000})}
      : {ok:false,status:500,json:async()=>({error:'failed'})};
  await check.demoNext();
  assert.equal(check.getDemo().step,0);
  assert.match(element('demoProof').textContent,/Initial write: HTTP 500/);
  assert.equal(element('btnDemoNext').disabled,false);
});


test('successful deletion records DELETE and clears the old paired-copy display',async()=>{
  const {check,element,context}=dashboard();check.setStatus(state());check.setSelected('observation');
  element('copyPair').hidden=false;element('detailOut').textContent='old full value';
  context.fetch=async(url,options)=>url==='/key/observation' && options.method==='DELETE'
    ? {ok:true,status:204,json:async()=>{throw new SyntaxError('No content')}}
    : {ok:false,status:503,json:async()=>({})};
  await element('btnDelete').onclick();
  assert.equal(element('copyPair').hidden,true);
  assert.equal(element('detailOut').textContent,'—');
  assert.equal(element('requestRoute').textContent,'DELETE /key/observation');
  assert.match(element('requestOutcome').textContent,/HTTP 204/);
});

test('a failed deletion keeps the selected value available for inspection',async()=>{
  const {check,element,context}=dashboard();check.setStatus(state());check.setSelected('observation');
  element('copyPair').hidden=false;element('detailOut').textContent='keep this value';
  context.fetch=async()=>({ok:false,status:503,json:async()=>({error:'unavailable'})});
  await element('btnDelete').onclick();
  assert.equal(element('copyPair').hidden,false);
  assert.equal(element('detailOut').textContent,'keep this value');
  assert.match(element('requestOutcome').textContent,/HTTP 503/);
});
