const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function dashboard() {
  let source = fs.readFileSync(path.join(__dirname, '../ui/index.html'), 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
  class Element {
    constructor(id = '',tag = '') {
      this.id=id;this.tag=tag;this.children=[];this.attributes={};this.style={};this.dataset={};this.value='';this.textContent='';this.className='';this.hidden=false;this.disabled=false;this.offsetWidth=300;
      this.parentElement={clientWidth:400};
      this.classList={contains:name=>this.className.split(' ').includes(name),add(){},remove(){},toggle(){}};
    }
    set innerHTML(value){this.html=value;this.children=[];}
    get innerHTML(){return this.html || '';}
    addEventListener(){}
    setAttribute(key,value){this.attributes[key]=String(value);}
    appendChild(child){this.children.push(child);return child;}
    getBoundingClientRect(){return {width:900,height:640};}
    animate(frames,options){this.animation={frames,options};return {cancel(){}};}
  }
  const elements=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,new Element(id));return elements.get(id);};
  const context={
    document:{getElementById:element,createElementNS:(_ns,tag)=>new Element('',tag),querySelectorAll:()=>[],activeElement:null},
    fetch:async()=>{throw Error('offline')},AbortSignal,console,localStorage:{getItem:()=>'',setItem(){}},
    setInterval(){},clearInterval(){},setTimeout(){},clearTimeout(){},addEventListener(){},
    location:{hash:''},getComputedStyle:()=>({position:'relative'}),matchMedia:()=>({matches:false}),
    innerWidth:1400,innerHeight:1000,confirm:()=>true,prompt:()=>'',
  };
  const bootstrap='  poll(); loadPrompt();\n  setInterval(poll, POLL_MS);';
  assert.ok(source.includes(bootstrap));
  source=source.replace(bootstrap,'  globalThis.check = { transfer, observeReplication, poll, render, renderTable, api, setStatus(value){status=value;}, setSelected(value){selected=value;} };\n  setInterval(poll, POLL_MS);');
  vm.createContext(context);vm.runInContext(source,context);
  return {context,element,check:context.check};
}
function state(key='observation') {
  return {keys:{[key]:{version:1,acked_version:1,residence:'RESIDENT_FULL',consistency_status:'SYNCED_ARCHIVED',active_tokens_in_node_a:200,full_tokens_on_node_b:200,eviction_allowed:true,pinned:false,priority:0}},active_window_tokens:210,max_window_tokens:2000,over_budget:false,page_faults:0,tokens_saved_by_replication:0,replication_successes:1,replication_failures:0,pending_deletes:0,local_value_count:1};
}
function descendants(element) {return element.children.flatMap(child=>[child,...descendants(child)]);}

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
