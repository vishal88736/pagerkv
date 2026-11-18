// Execute the dashboard's actual guided-flow code against isolated real services.
const assert = require('node:assert/strict');
const {dashboard} = require('./dashboard-harness.cjs');
const base = process.env.PAGERKV_TEST_A_URL;
assert.match(base || '', /^http:\/\/127\.0\.0\.1:\d+$/);

async function json(route, options={}) {
  const response=await fetch(base+route,{...options,headers:{'content-type':'application/json'}});
  assert.ok(response.ok,`${route}: HTTP ${response.status}`);
  return response.status===204 ? null : response.json();
}

(async()=>{
  await json('/key',{method:'PUT',body:JSON.stringify({key:'existing_user_value',value:'leave this unchanged'})});
  const {check,element,context}=dashboard({baseUrl:base,liveTimers:true});
  const calls=[];
  const realFetch=context.fetch;
  context.fetch=(route,options)=>{calls.push({route,method:options?.method || 'GET'});return realFetch(route,options);};
  await check.poll();
  for(let step=0;step<4;step++) {
    const before=calls.filter(call=>call.method==='PUT').length;
    await check.demoNext();
    assert.equal(check.getDemo()?.step,step+1,element('demoProof').textContent);
    if(step===0) {
      const key=check.getDemo().key;
      const pair=await json('/replication/key/'+key);
      assert.equal(pair.node_a.version,pair.node_b.version);
      assert.equal(pair.node_a.residence,'RESIDENT_FULL');
    }
    if(step===1) {
      const result=await json('/replication/status');
      assert.equal(result.comparison_status,'unknown');
      assert.equal(result.last_replication.http_status,503);
      assert.equal(result.last_replication.ok,false);
    }
    if(step===2) assert.match(element('demoProof').textContent,/LOCAL_STORE/);
    if(step===3) {
      assert.equal(calls.filter(call=>call.method==='PUT').length,before,'Recovery must use background retry, not another PUT');
      assert.equal((await json('/replication/status')).last_replication.source,'background_retry');
    }
  }
  await check.demoNext();
  assert.equal(check.getDemo().step,4,element('demoProof').textContent);
  assert.equal(check.getDemo().pageReady,true);
  const sample=check.getDemo().pagedKey;
  let pair=await json('/replication/key/'+sample);
  assert.equal(pair.node_a.residence,'PAGED_TO_NODE_B');
  assert.equal(pair.node_a.value,pair.node_b.value);
  assert.equal(pair.comparison_status,'in_sync');
  assert.match(element('pairContext').textContent,/paged stub/);
  assert.match(element('btnDemoNext').textContent,/Hydrate/);
  const before=(await json('/context/status?include_text=false')).page_faults;
  await check.demoNext();
  assert.equal(check.getDemo().step,5,element('demoProof').textContent);
  assert.ok((await json('/context/status?include_text=false')).page_faults>before);
  pair=await json('/replication/key/'+sample);
  assert.equal(pair.node_a.value,pair.node_b.value);
  assert.match(element('demoProof').textContent,/hydrated exactly/);
  const prefix=check.getDemo().prefix;
  await check.demoReset();
  assert.equal(check.getDemo(),null,element('demoProof').textContent);
  assert.deepEqual(Object.keys((await json('/keys')).keys),['existing_user_value']);
  assert.ok(calls.filter(call=>call.method==='DELETE').every(call=>decodeURIComponent(call.route).includes('/'+prefix+'_')));
  assert.equal((await json('/key/existing_user_value')).value,'leave this unchanged');
  console.log('Live guided demo passed: real REST copy, HTTP 503, local read, background recovery, distinct paging pause, exact hydration, scoped cleanup.');

  await check.demoNext();await check.demoNext();
  assert.equal((await json('/admin/node_b/fault')).mode,'down');
  await check.demoReset();
  assert.equal(check.getDemo(),null,element('demoProof').textContent);
  assert.equal((await json('/admin/node_b/fault')).mode,'none');
  assert.deepEqual(Object.keys((await json('/keys')).keys),['existing_user_value']);
  console.log('Live interrupted-demo reset passed: B restored and pre-existing user value preserved.');
})().catch(error=>{console.error(error);process.exitCode=1;});
