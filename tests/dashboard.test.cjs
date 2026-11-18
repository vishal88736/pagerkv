const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function dashboard() {
  const html = fs.readFileSync(path.join(__dirname, '../ui/index.html'), 'utf8');
  let source = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {value:'',textContent:'',innerHTML:'',className:'',dataset:{},style:{},hidden:false,disabled:false,parentElement:{clientWidth:400},addEventListener(){},animate(frames,options){this.animation={frames,options};return {cancel(){}}}});
    return elements.get(id);
  };
  const context = {
    document:{getElementById:element},
    fetch:async()=>{throw new Error('offline')},
    AbortSignal,console,confirm:()=>true,prompt:()=>'',
    setInterval(){},matchMedia:()=>({matches:false}),
  };
  const bootstrap = 'poll(); readFault(); setInterval(poll,2000);';
  assert.ok(source.includes(bootstrap));
  source = source.replace(bootstrap,'globalThis.check = {render, animate, api, poll, operate};');
  vm.createContext(context); vm.runInContext(source,context);
  return {context, element, check:context.check};
}
function snapshot(value='Bhilai') {
  const row = {key:'city',value,version:1,acked_version:1,deleted:0,error:null};
  return {consistency:'in_sync',node_a:{city:row},node_b:{city:{...row}},pending:0,replication:{attempts:1,succeeded:1,failed:0}};
}

test('both value tables show complete copies and escape user input',()=>{
  const {check,element}=dashboard();
  check.render(snapshot('<script>alert(1)</script>'));
  for (const id of ['rowsA','rowsB']) {
    assert.match(element(id).innerHTML,/&lt;script&gt;alert/);
    assert.doesNotMatch(element(id).innerHTML,/<script>/);
  }
  assert.equal(element('consistency').textContent,'In sync');
  assert.equal(element('keyCount').textContent,1);
});

test('unavailable replica is unknown and does not display invented contents',()=>{
  const {check,element}=dashboard();
  const data=snapshot();data.node_b=null;data.consistency='unknown';data.pending=1;
  check.render(data);
  assert.match(element('consistency').textContent,/Unknown/);
  assert.match(element('rowsB').innerHTML,/cannot be verified/);
  assert.equal(element('pendingCount').textContent,1);
});

test('copy animation runs while pending and ends on acknowledgment',()=>{
  const {check,element}=dashboard();
  check.animate('pending','Sending city');
  assert.equal(element('packet').animation.options.iterations,Infinity);
  check.animate('ok','Acknowledged city');
  assert.equal(element('packet').animation.options.iterations,1);
  assert.equal(element('flow').dataset.state,'ok');
  assert.equal(element('activity').textContent,'Acknowledged city');
});

test('reduced motion disables movement',()=>{
  const {check,element,context}=dashboard();
  context.matchMedia=()=>({matches:true});check.animate('ok','Copy complete');
  assert.equal(element('packet').animation,undefined);
  assert.equal(element('activity').textContent,'Copy complete');
});

test('HTTP failure marks the previous snapshot stale and recovery clears banner',async()=>{
  const {check,element,context}=dashboard();
  check.render(snapshot());
  context.fetch=async()=>({ok:false,status:500,json:async()=>({})});
  await check.poll();
  assert.equal(element('banner').hidden,false);
  assert.match(element('live').textContent,/stale/);
  assert.match(element('rowsA').innerHTML,/Bhilai/);
  context.fetch=async()=>({ok:true,status:200,json:async()=>snapshot()});
  await check.poll();assert.equal(element('banner').hidden,true);
});

test('a dashboard PUT uses the REST value body and reports failed replication',async()=>{
  const {check,element,context}=dashboard();
  element('key').value='city';element('value').value='Raipur';
  let write;
  const data=snapshot('Raipur');data.consistency='unknown';data.node_b=null;data.pending=1;data.node_a.city.acked_version=0;
  context.fetch=async(url,options)=>{
    if(url==='/key/city') {
      write=JSON.parse(options.body);
      return {ok:true,status:200,json:async()=>({version:1,replicated:false,deleted:0})};
    }
    return {ok:true,status:200,json:async()=>data};
  };
  await check.operate('PUT');
  assert.equal(write.value,'Raipur');
  assert.equal(element('flow').dataset.state,'bad');
  assert.match(element('activity').textContent,/saved on A/);
  assert.equal(element('put').disabled,false);
  assert.match(element('response').textContent,/PUT \/key\/city/);
});
