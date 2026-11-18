const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function dashboard(options = {}) {
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
    fetch: options.baseUrl ? (url, settings) => fetch(options.baseUrl + url, settings) : async()=>{throw Error('offline')},AbortSignal,console,localStorage:{getItem:()=>'',setItem(){}},
    setInterval(){},clearInterval(){},setTimeout: options.liveTimers ? (callback, delay) => delay < 1000 ? setTimeout(callback, delay) : null : ()=>null,clearTimeout,addEventListener(){},
    location:{hash:''},getComputedStyle:()=>({position:'relative'}),matchMedia:()=>({matches:false}),
    innerWidth:1400,innerHeight:1000,confirm:()=>true,prompt:()=>'',
  };
  const bootstrap='  poll(); loadPrompt();\n  setInterval(poll, POLL_MS);';
  assert.ok(source.includes(bootstrap));
  source=source.replace(bootstrap,'  globalThis.check = { transfer, observeReplication, poll, render, renderTable, api, writeValue, observedRead, renderReplicationResult, renderCopyPair, loadCopyPair, demoNext, demoReset, getDemo(){return demo;}, setStatus(value){status=value;}, setSelected(value){selected=value;} };\n  setInterval(poll, POLL_MS);');
  vm.createContext(context);vm.runInContext(source,context);
  return {context,element,check:context.check};
}
function state(key='observation') {
  return {keys:{[key]:{version:1,acked_version:1,residence:'RESIDENT_FULL',consistency_status:'SYNCED_ARCHIVED',active_tokens_in_node_a:200,full_tokens_on_node_b:200,eviction_allowed:true,pinned:false,priority:0}},active_window_tokens:210,max_window_tokens:2000,over_budget:false,page_faults:0,tokens_saved_by_replication:0,replication_successes:1,replication_failures:0,pending_deletes:0,local_value_count:1};
}
function descendants(element) {return element.children.flatMap(child=>[child,...descendants(child)]);}


module.exports = {dashboard, state, descendants};
