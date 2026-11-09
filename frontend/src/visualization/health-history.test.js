import test from 'node:test';
import assert from 'node:assert/strict';
import { HealthHistory } from '../health-history.js';
const at = Date.now();
const observation = (service,live,time=at) => ({service,live,at:time,durationMs:12,httpStatus:live?200:503,detail:'Test'});
test('service transitions are independent and filterable',()=>{
 const history = new HealthHistory();
 history.add(observation('generator',true));history.add(observation('backend',true));
 history.add(observation('generator',false,at+10));history.add(observation('generator',false,at+20));
 history.add(observation('generator',true,at+30));
 assert.equal(history.select('generator',true).length,3);
 assert.equal(history.select('generator')[0].transition,'Recovered');
 assert.equal(history.select('backend').length,1);
});
test('retains at most 1000 checks and discards old observations',()=>{
 const history = new HealthHistory();
 for(let i=0;i<1005;i++)history.add(observation('backend',true,at+i));
 assert.equal(history.records.length,1000);
 history.prune(at+25*60*60*1000);assert.equal(history.records.length,0);
});
test('persists checks across reloads and tolerates unavailable storage',()=>{
 const store={value:null,getItem(){return this.value},setItem(key,value){this.value=value}};
 const history=new HealthHistory(store);history.add(observation('generator',false));
 assert.equal(new HealthHistory(store).records.length,1);
 const broken=new HealthHistory({getItem(){throw Error('denied')},setItem(){throw Error('denied')}});
 broken.add(observation('backend',true));assert.equal(broken.records.length,1);assert.equal(broken.persistent,false);
});
