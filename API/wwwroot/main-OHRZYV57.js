var Kk=Object.defineProperty,Xk=Object.defineProperties;var Jk=Object.getOwnPropertyDescriptors;var Ry=Object.getOwnPropertySymbols;var eM=Object.prototype.hasOwnProperty,tM=Object.prototype.propertyIsEnumerable;var Ny=(t,n,e)=>n in t?Kk(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,b=(t,n)=>{for(var e in n||={})eM.call(n,e)&&Ny(t,e,n[e]);if(Ry)for(var e of Ry(n))tM.call(n,e)&&Ny(t,e,n[e]);return t},Q=(t,n)=>Xk(t,Jk(n));var Ls=(t=>typeof require<"u"?require:typeof Proxy<"u"?new Proxy(t,{get:(n,e)=>(typeof require<"u"?require:n)[e]}):t)(function(t){if(typeof require<"u")return require.apply(this,arguments);throw Error('Dynamic require of "'+t+'" is not supported')});var mn=null,yd=!1,Ro=1,nM=null,wt=Symbol("SIGNAL");function te(t){let n=mn;return mn=t,n}function Cd(){return mn}var Lr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Br(t){if(yd)throw new Error("");if(mn===null)return;mn.consumerOnSignalRead(t);let n=mn.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=mn.recomputing;if(i&&(e=n!==void 0?n.nextProducer:mn.producers,e!==void 0&&e.producer===t)){mn.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=Ro;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===mn&&(!i||r.knownValidAtEpoch===Ro))return;let o=js(mn),s={producer:t,consumer:mn,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:Ro,lastReadVersion:t.version,nextConsumer:void 0};mn.producersTail=s,n!==void 0?n.nextProducer=s:mn.producers=s,o&&Ly(t,s)}function Oy(){Ro++}function Po(t){if(!(js(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Ro)){if(!t.producerMustRecompute(t)&&!Vs(t)){Bs(t);return}t.producerRecomputeValue(t),Bs(t)}}function cp(t){if(t.consumers===void 0)return;let n=yd;yd=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||iM(i)}}finally{yd=n}}function lp(){return mn?.consumerAllowSignalWrites!==!1}function iM(t){t.dirty=!0,cp(t),t.consumerMarkedDirty?.(t)}function Bs(t){t.dirty=!1,t.lastCleanEpoch=Ro}function lr(t){return t&&Py(t),te(t)}function Py(t){if(t.producersTail?.knownValidAtEpoch===Ro){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Vr(t,n){te(n),t&&Fy(t)}function Fy(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(js(t))do e=dp(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Vs(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Po(e),i!==e.version))return!0}return!1}function jr(t){if(js(t)){let n=t.producers;for(;n!==void 0;)n=dp(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Ly(t,n){let e=t.consumersTail,i=js(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Ly(r.producer,r)}function dp(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!js(n)){let o=n.producers;for(;o!==void 0;)o=dp(o)}return e}function js(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function sc(t){nM?.(t)}function ac(t,n){return Object.is(t,n)}function cc(t,n){let e=Object.create(rM);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Po(e),Br(e),e.value===Ti)throw e.error;return e.value};return i[wt]=e,sc(e),i}var No=Symbol("UNSET"),Oo=Symbol("COMPUTING"),Ti=Symbol("ERRORED"),rM=Q(b({},Lr),{value:No,dirty:!0,error:null,equal:ac,kind:"computed",producerMustRecompute(t){return t.value===No||t.value===Oo},producerRecomputeValue(t){if(t.value===Oo)throw new Error("");let n=t.value;t.value=Oo;let e=lr(t),i,r=!1;try{i=t.computation(),te(null),r=n!==No&&n!==Ti&&i!==Ti&&t.equal(n,i)}catch(o){i=Ti,t.error=o}finally{Vr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function oM(){throw new Error}var By=oM;function Vy(t){By(t)}function up(t){By=t}var sM=null;function mp(t,n){let e=Object.create(lc);e.value=t,n!==void 0&&(e.equal=n);let i=()=>jy(e);return i[wt]=e,sc(e),[i,s=>Fo(e,s),s=>wd(e,s)]}function jy(t){return Br(t),t.value}function Fo(t,n){lp()||Vy(t),t.equal(t.value,n)||(t.value=n,aM(t))}function wd(t,n){lp()||Vy(t),Fo(t,n(t.value))}var lc=Q(b({},Lr),{equal:ac,value:void 0,kind:"signal"});function aM(t){t.version++,Oy(),cp(t),sM?.(t)}var fp=Q(b({},Lr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function hp(t){if(t.dirty=!1,t.version>0&&!Vs(t))return;t.version++;let n=lr(t);try{t.cleanup(),t.fn()}finally{Vr(t,n)}}var pp;function dc(){return pp}function Ai(t){let n=pp;return pp=t,n}function xd(t,n){let e=dc();if(!e)throw new Error("Current injector is not set.");if(!t.\u0275prov)throw new Error("Token is not an injectable");return e.retrieve(t,n)}var Hy=Symbol("NotFound");function Hs(t){return t===Hy||t?.name==="\u0275NotFound"}function gp(t,n,e){let i=Object.create(cM);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Po(i),Br(i),i.value===Ti)throw i.error;return i.value};return o[wt]=i,sc(i),o}function Uy(t,n){Po(t),Fo(t,n),Bs(t)}function zy(t,n){if(Po(t),t.value===Ti)throw t.error;wd(t,n),Bs(t)}var cM=Q(b({},Lr),{value:No,dirty:!0,error:null,equal:ac,kind:"linkedSignal",producerMustRecompute(t){return t.value===No||t.value===Oo},producerRecomputeValue(t){if(t.value===Oo)throw new Error("");let n=t.value;t.value=Oo;let e=lr(t),i,r=!1;try{let o=t.source(),s=n!==No&&n!==Ti,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,te(null),r=s&&i!==Ti&&t.equal(n,i)}catch(o){i=Ti,t.error=o}finally{Vr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function $y(t){let n=te(null);try{return t()}finally{te(n)}}function fe(t){return typeof t=="function"}function Us(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Dd=Us(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Lo(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var he=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(fe(i))try{i()}catch(o){n=o instanceof Dd?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Gy(o)}catch(s){n=n??[],s instanceof Dd?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Dd(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Gy(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Lo(e,n)}remove(n){let{_finalizers:e}=this;e&&Lo(e,n),n instanceof t&&n._removeParent(this)}};he.EMPTY=(()=>{let t=new he;return t.closed=!0,t})();var _p=he.EMPTY;function Ed(t){return t instanceof he||t&&"closed"in t&&fe(t.remove)&&fe(t.add)&&fe(t.unsubscribe)}function Gy(t){fe(t)?t():t.unsubscribe()}var ui={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var zs={setTimeout(t,n,...e){let{delegate:i}=zs;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=zs;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Sd(t){zs.setTimeout(()=>{let{onUnhandledError:n}=ui;if(n)n(t);else throw t})}function uc(){}var Wy=vp("C",void 0,void 0);function qy(t){return vp("E",void 0,t)}function Yy(t){return vp("N",t,void 0)}function vp(t,n,e){return{kind:t,value:n,error:e}}var Bo=null;function $s(t){if(ui.useDeprecatedSynchronousErrorHandling){let n=!Bo;if(n&&(Bo={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Bo;if(Bo=null,e)throw i}}else t()}function Zy(t){ui.useDeprecatedSynchronousErrorHandling&&Bo&&(Bo.errorThrown=!0,Bo.error=t)}var Vo=class extends he{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Ed(n)&&n.add(this)):this.destination=uM}static create(n,e,i){return new mi(n,e,i)}next(n){this.isStopped?yp(Yy(n),this):this._next(n)}error(n){this.isStopped?yp(qy(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?yp(Wy,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},lM=Function.prototype.bind;function bp(t,n){return lM.call(t,n)}var Cp=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Id(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Id(i)}else Id(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Id(e)}}},mi=class extends Vo{constructor(n,e,i){super();let r;if(fe(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&ui.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&bp(n.next,o),error:n.error&&bp(n.error,o),complete:n.complete&&bp(n.complete,o)}):r=n}this.destination=new Cp(r)}};function Id(t){ui.useDeprecatedSynchronousErrorHandling?Zy(t):Sd(t)}function dM(t){throw t}function yp(t,n){let{onStoppedNotification:e}=ui;e&&zs.setTimeout(()=>e(t,n))}var uM={closed:!0,next:uc,error:dM,complete:uc};var Gs=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Nn(t){return t}function wp(...t){return xp(t)}function xp(t){return t.length===0?Nn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var le=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=fM(e)?e:new mi(e,i,r);return $s(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Qy(i),new i((r,o)=>{let s=new mi({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Gs](){return this}pipe(...e){return xp(e)(this)}toPromise(e){return e=Qy(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Qy(t){var n;return(n=t??ui.Promise)!==null&&n!==void 0?n:Promise}function mM(t){return t&&fe(t.next)&&fe(t.error)&&fe(t.complete)}function fM(t){return t&&t instanceof Vo||mM(t)&&Ed(t)}function hM(t){return fe(t?.lift)}function _e(t){return n=>{if(hM(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ye(t,n,e,i,r){return new Dp(t,n,e,i,r)}var Dp=class extends Vo{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Ky=Us(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var S=(()=>{class t extends le{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new kd(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new Ky}next(e){$s(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){$s(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){$s(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?_p:(this.currentObservers=null,o.push(e),new he(()=>{this.currentObservers=null,Lo(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new le;return e.source=this,e}}return t.create=(n,e)=>new kd(n,e),t})(),kd=class extends S{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:_p}};var Pt=class extends S{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var mc={now(){return(mc.delegate||Date).now()},delegate:void 0};var Ri=class extends S{constructor(n=1/0,e=1/0,i=mc){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Md=class extends he{constructor(n,e){super()}schedule(n,e=0){return this}};var fc={setInterval(t,n,...e){let{delegate:i}=fc;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=fc;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Td=class extends Md{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return fc.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&fc.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Lo(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Ws=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Ws.now=mc.now;var Ad=class extends Ws{constructor(n,e=Ws.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var hc=new Ad(Td),Xy=hc;var xt=new le(t=>t.complete());function Rd(t){return t&&fe(t.schedule)}function Ep(t){return t[t.length-1]}function Nd(t){return fe(Ep(t))?t.pop():void 0}function Ni(t){return Rd(Ep(t))?t.pop():void 0}function Jy(t,n){return typeof Ep(t)=="number"?t.pop():n}function t0(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{l(i.next(u))}catch(m){s(m)}}function c(u){try{l(i.throw(u))}catch(m){s(m)}}function l(u){u.done?o(u.value):r(u.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function e0(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function jo(t){return this instanceof jo?(this.v=t,this):new jo(t)}function n0(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(_){return function(C){return Promise.resolve(C).then(_,m)}}function a(_,C){i[_]&&(r[_]=function(I){return new Promise(function(j,se){o.push([_,I,j,se])>1||c(_,I)})},C&&(r[_]=C(r[_])))}function c(_,C){try{l(i[_](C))}catch(I){g(o[0][3],I)}}function l(_){_.value instanceof jo?Promise.resolve(_.value.v).then(u,m):g(o[0][2],_)}function u(_){c("next",_)}function m(_){c("throw",_)}function g(_,C){_(C),o.shift(),o.length&&c(o[0][0],o[0][1])}}function i0(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof e0=="function"?e0(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Od=t=>t&&typeof t.length=="number"&&typeof t!="function";function Pd(t){return fe(t?.then)}function Fd(t){return fe(t[Gs])}function Ld(t){return Symbol.asyncIterator&&fe(t?.[Symbol.asyncIterator])}function Bd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function pM(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Vd=pM();function jd(t){return fe(t?.[Vd])}function Hd(t){return n0(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield jo(e.read());if(r)return yield jo(void 0);yield yield jo(i)}}finally{e.releaseLock()}})}function Ud(t){return fe(t?.getReader)}function Ze(t){if(t instanceof le)return t;if(t!=null){if(Fd(t))return gM(t);if(Od(t))return _M(t);if(Pd(t))return vM(t);if(Ld(t))return r0(t);if(jd(t))return bM(t);if(Ud(t))return yM(t)}throw Bd(t)}function gM(t){return new le(n=>{let e=t[Gs]();if(fe(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function _M(t){return new le(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function vM(t){return new le(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Sd)})}function bM(t){return new le(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function r0(t){return new le(n=>{CM(t,n).catch(e=>n.error(e))})}function yM(t){return r0(Hd(t))}function CM(t,n){var e,i,r,o;return t0(this,void 0,void 0,function*(){try{for(e=i0(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function xn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function zd(t,n=0){return _e((e,i)=>{e.subscribe(ye(i,r=>xn(i,t,()=>i.next(r),n),()=>xn(i,t,()=>i.complete(),n),r=>xn(i,t,()=>i.error(r),n)))})}function $d(t,n=0){return _e((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function o0(t,n){return Ze(t).pipe($d(n),zd(n))}function s0(t,n){return Ze(t).pipe($d(n),zd(n))}function a0(t,n){return new le(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function c0(t,n){return new le(e=>{let i;return xn(e,n,()=>{i=t[Vd](),xn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>fe(i?.return)&&i.return()})}function Gd(t,n){if(!t)throw new Error("Iterable cannot be null");return new le(e=>{xn(e,n,()=>{let i=t[Symbol.asyncIterator]();xn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function l0(t,n){return Gd(Hd(t),n)}function d0(t,n){if(t!=null){if(Fd(t))return o0(t,n);if(Od(t))return a0(t,n);if(Pd(t))return s0(t,n);if(Ld(t))return Gd(t,n);if(jd(t))return c0(t,n);if(Ud(t))return l0(t,n)}throw Bd(t)}function ot(t,n){return n?d0(t,n):Ze(t)}function W(...t){let n=Ni(t);return ot(t,n)}function pc(t,n){let e=fe(t)?t:()=>t,i=r=>r.error(e());return new le(n?r=>n.schedule(i,0,r):i)}function gc(t){return!!t&&(t instanceof le||fe(t.lift)&&fe(t.subscribe))}var fi=Us(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Sp(t,n){let e=typeof n=="object";return new Promise((i,r)=>{let o=!1,s;t.subscribe({next:a=>{s=a,o=!0},error:r,complete:()=>{o?i(s):e?i(n.defaultValue):r(new fi)}})})}function Ho(t,n){let e=typeof n=="object";return new Promise((i,r)=>{let o=new mi({next:s=>{i(s),o.unsubscribe()},error:r,complete:()=>{e?i(n.defaultValue):r(new fi)}});t.subscribe(o)})}function u0(t){return t instanceof Date&&!isNaN(t)}function Y(t,n){return _e((e,i)=>{let r=0;e.subscribe(ye(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:wM}=Array;function xM(t,n){return wM(n)?t(...n):t(n)}function Wd(t){return Y(n=>xM(t,n))}var{isArray:DM}=Array,{getPrototypeOf:EM,prototype:SM,keys:IM}=Object;function qd(t){if(t.length===1){let n=t[0];if(DM(n))return{args:n,keys:null};if(kM(n)){let e=IM(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function kM(t){return t&&typeof t=="object"&&EM(t)===SM}function Yd(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function _c(...t){let n=Ni(t),e=Nd(t),{args:i,keys:r}=qd(t);if(i.length===0)return ot([],n);let o=new le(MM(i,n,r?s=>Yd(r,s):Nn));return e?o.pipe(Wd(e)):o}function MM(t,n,e=Nn){return i=>{m0(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)m0(n,()=>{let l=ot(t[c],n),u=!1;l.subscribe(ye(i,m=>{o[c]=m,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function m0(t,n,e){t?xn(e,t,n):n()}function f0(t,n,e,i,r,o,s,a){let c=[],l=0,u=0,m=!1,g=()=>{m&&!c.length&&!l&&n.complete()},_=I=>l<i?C(I):c.push(I),C=I=>{o&&n.next(I),l++;let j=!1;Ze(e(I,u++)).subscribe(ye(n,se=>{r?.(se),o?_(se):n.next(se)},()=>{j=!0},void 0,()=>{if(j)try{for(l--;c.length&&l<i;){let se=c.shift();s?xn(n,s,()=>C(se)):C(se)}g()}catch(se){n.error(se)}}))};return t.subscribe(ye(n,_,()=>{m=!0,g()})),()=>{a?.()}}function Kt(t,n,e=1/0){return fe(n)?Kt((i,r)=>Y((o,s)=>n(i,o,r,s))(Ze(t(i,r))),e):(typeof n=="number"&&(e=n),_e((i,r)=>f0(i,r,t,e)))}function Hr(t=1/0){return Kt(Nn,t)}function h0(){return Hr(1)}function Ur(...t){return h0()(ot(t,Ni(t)))}function hi(t){return new le(n=>{Ze(t()).subscribe(n)})}function Uo(...t){let n=Nd(t),{args:e,keys:i}=qd(t),r=new le(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let u=0;u<s;u++){let m=!1;Ze(e[u]).subscribe(ye(o,g=>{m||(m=!0,l--),a[u]=g},()=>c--,void 0,()=>{(!c||!m)&&(l||o.next(i?Yd(i,a):a),o.complete())}))}});return n?r.pipe(Wd(n)):r}function p0(t=0,n,e=Xy){let i=-1;return n!=null&&(Rd(n)?e=n:i=n),new le(r=>{let o=u0(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function fn(...t){let n=Ni(t),e=Jy(t,1/0),i=t;return i.length?i.length===1?Ze(i[0]):Hr(e)(ot(i,n)):xt}function Ie(t,n){return _e((e,i)=>{let r=0;e.subscribe(ye(i,o=>t.call(n,o,r++)&&i.next(o)))})}function g0(t){return _e((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(ye(e,l=>{i=!0,r=l,o||Ze(t(l)).subscribe(o=ye(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function Zd(t,n=hc){return g0(()=>p0(t,n))}function zr(t){return _e((n,e)=>{let i=null,r=!1,o;i=n.subscribe(ye(e,void 0,void 0,s=>{o=Ze(t(s,zr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function zo(t,n){return fe(n)?Kt(t,n,1):Kt(t,1)}function $o(t,n=hc){return _e((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,u=n.now();if(u<l){r=this.schedule(void 0,l-u),i.add(r);return}a()}e.subscribe(ye(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function _0(t){return _e((n,e)=>{let i=!1;n.subscribe(ye(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function mt(t){return t<=0?()=>xt:_e((n,e)=>{let i=0;n.subscribe(ye(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Qd(t,n=Nn){return t=t??TM,_e((e,i)=>{let r,o=!0;e.subscribe(ye(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function TM(t,n){return t===n}function v0(t=AM){return _e((n,e)=>{let i=!1;n.subscribe(ye(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function AM(){return new fi}function Go(t){return _e((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function dr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Ie((r,o)=>t(r,o,i)):Nn,mt(1),e?_0(n):v0(()=>new fi))}function Kd(t){return t<=0?()=>xt:_e((n,e)=>{let i=[];n.subscribe(ye(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Xd(){return _e((t,n)=>{let e,i=!1;t.subscribe(ye(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function vc(t={}){let{connector:n=()=>new S,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,u=!1,m=!1,g=()=>{a?.unsubscribe(),a=void 0},_=()=>{g(),s=c=void 0,u=m=!1},C=()=>{let I=s;_(),I?.unsubscribe()};return _e((I,j)=>{l++,!m&&!u&&g();let se=c=c??n();j.add(()=>{l--,l===0&&!m&&!u&&(a=Ip(C,r))}),se.subscribe(j),!s&&l>0&&(s=new mi({next:Ne=>se.next(Ne),error:Ne=>{m=!0,g(),a=Ip(_,e,Ne),se.error(Ne)},complete:()=>{u=!0,g(),a=Ip(_,i),se.complete()}}),Ze(I).subscribe(s))})(o)}}function Ip(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new mi({next:()=>{i.unsubscribe(),t()}});return Ze(n(...e)).subscribe(i)}function Jd(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,vc({connector:()=>new Ri(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function bc(t){return Ie((n,e)=>t<=e)}function et(...t){let n=Ni(t);return _e((e,i)=>{(n?Ur(t,e,n):Ur(t,e)).subscribe(i)})}function pt(t,n){return _e((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(ye(i,c=>{r?.unsubscribe();let l=0,u=o++;Ze(t(c,u)).subscribe(r=ye(i,m=>i.next(n?n(c,m,u,l++):m),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function pe(t){return _e((n,e)=>{Ze(t).subscribe(ye(e,()=>e.complete(),uc)),!e.closed&&n.subscribe(e)})}function kp(t,n=!1){return _e((e,i)=>{let r=0;e.subscribe(ye(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function tt(t,n,e){let i=fe(t)||n||e?{next:t,error:n,complete:e}:t;return i?_e((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(ye(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Nn}var su="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",k=class extends Error{code;constructor(n,e){super(Pi(n,e)),this.code=n}};function RM(t){return`NG0${Math.abs(t)}`}function Pi(t,n){return`${RM(t)}${n?": "+n:""}`}function Fe(t){for(let n in t)if(t[n]===Fe)return n;throw Error("")}function x0(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Ec(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Ec).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function au(t,n){return t?n?`${t} ${n}`:t:n||""}var NM=Fe({__forward_ref__:Fe});function Mt(t){return t.__forward_ref__=Mt,t}function Yt(t){return Hp(t)?t():t}function Hp(t){return typeof t=="function"&&t.hasOwnProperty(NM)&&t.__forward_ref__===Mt}function J(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function ce(t){return{providers:t.providers||[],imports:t.imports||[]}}function Sc(t){return OM(t,cu)}function Up(t){return Sc(t)!==null}function OM(t,n){return t.hasOwnProperty(n)&&t[n]||null}function PM(t){let n=t?.[cu]??null;return n||null}function Tp(t){return t&&t.hasOwnProperty(tu)?t[tu]:null}var cu=Fe({\u0275prov:Fe}),tu=Fe({\u0275inj:Fe}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=J({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function zp(t){return t&&!!t.\u0275providers}var $p=Fe({\u0275cmp:Fe}),Gp=Fe({\u0275dir:Fe}),Wp=Fe({\u0275pipe:Fe}),qp=Fe({\u0275mod:Fe}),Cc=Fe({\u0275fac:Fe}),Qo=Fe({__NG_ELEMENT_ID__:Fe}),b0=Fe({__NG_ENV_ID__:Fe});function D0(t){return lu(t,"@NgModule"),t[qp]||null}function Wr(t){return lu(t,"@Component"),t[$p]||null}function Yp(t){return lu(t,"@Directive"),t[Gp]||null}function E0(t){return lu(t,"@Pipe"),t[Wp]||null}function lu(t,n){if(t==null)throw new k(-919,!1)}function Ko(t){return typeof t=="string"?t:t==null?"":String(t)}var S0=Fe({ngErrorCode:Fe}),FM=Fe({ngErrorMessage:Fe}),LM=Fe({ngTokenPath:Fe});function Zp(t,n){return I0("",-200,n)}function du(t,n){throw new k(-201,!1)}function I0(t,n,e){let i=new k(n,t);return i[S0]=n,i[FM]=t,e&&(i[LM]=e),i}function BM(t){return t[S0]}var Ap;function k0(){return Ap}function hn(t){let n=Ap;return Ap=t,n}function Qp(t,n,e){let i=Sc(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;du(t,"")}var Wn=globalThis;var VM={},Wo=VM,jM="__NG_DI_FLAG__",Rp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=qo(e)||0;try{return this.injector.get(n,i&8?null:Wo,i)}catch(r){if(Hs(r))return r;throw r}}};function HM(t,n=0){let e=dc();if(e===void 0)throw new k(-203,!1);if(e===null)return Qp(t,void 0,n);{let i=UM(n),r=e.retrieve(t,i);if(Hs(r)){if(i.optional)return null;throw r}return r}}function ne(t,n=0){return(k0()||HM)(Yt(t),n)}function d(t,n){return ne(t,qo(n))}function qo(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function UM(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Np(t){let n=[];for(let e=0;e<t.length;e++){let i=Yt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new k(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=zM(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(ne(r,o))}else n.push(ne(i))}return n}function zM(t){return t[jM]}function $r(t,n){let e=t.hasOwnProperty(Cc);return e?t[Cc]:null}function M0(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function T0(t){return t.flat(Number.POSITIVE_INFINITY)}function uu(t,n){t.forEach(e=>Array.isArray(e)?uu(e,n):n(e))}function Kp(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Ic(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function A0(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function R0(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function mu(t,n,e){let i=Zs(t,n);return i>=0?t[i|1]=e:(i=~i,R0(t,i,n,e)),i}function fu(t,n){let e=Zs(t,n);if(e>=0)return t[e|1]}function Zs(t,n){return $M(t,n,1)}function $M(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var qr={},Xt=[],Xo=new y(""),kc=new y("",-1),Xp=new y(""),Ys=class{get(n,e=Wo){if(e===Wo){let r=I0("",-201);throw r.name="\u0275NotFound",r}return e}};function pi(t){return{\u0275providers:t}}function N0(t){return pi([{provide:Xo,multi:!0,useValue:t}])}function O0(...t){return{\u0275providers:Jp(!0,t),\u0275fromNgModule:!0}}function Jp(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return uu(n,s=>{let a=s;nu(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&P0(r,o),e}function P0(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];eg(r,o=>{n(o,i)})}}function nu(t,n,e,i){if(t=Yt(t),!t)return!1;let r=null,o=Tp(t),s=!o&&Wr(t);if(!o&&!s){let c=t.ngModule;if(o=Tp(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)nu(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;uu(o.imports,u=>{nu(u,n,e,i)&&(l||=[],l.push(u))}),l!==void 0&&P0(l,n)}if(!a){let l=$r(r)||(()=>new r);n({provide:r,useFactory:l,deps:Xt},r),n({provide:Xp,useValue:r,multi:!0},r),n({provide:Xo,useValue:()=>ne(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;eg(c,u=>{n(u,l)})}}else return!1;return r!==t&&t.providers!==void 0}function eg(t,n){for(let e of t)zp(e)&&(e=e.\u0275providers),Array.isArray(e)?eg(e,n):n(e)}var GM=Fe({provide:String,useValue:Fe});function F0(t){return t!==null&&typeof t=="object"&&GM in t}function WM(t){return!!(t&&t.useExisting)}function qM(t){return!!(t&&t.useFactory)}function Yo(t){return typeof t=="function"}function L0(t){return!!t.useClass}var Mc=new y(""),eu={},y0={},Mp;function Qs(){return Mp===void 0&&(Mp=new Ys),Mp}var Ue=class{},Zo=class extends Ue{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Pp(n,s=>this.processProvider(s)),this.records.set(kc,qs(void 0,this)),r.has("environment")&&this.records.set(Ue,qs(void 0,this));let o=this.records.get(Mc);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Xp,Xt,{self:!0}))}retrieve(n,e){let i=qo(e)||0;try{return this.get(n,Wo,i)}catch(r){if(Hs(r))return r;throw r}}destroy(){yc(this),this._destroyed=!0;let n=te(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),te(n)}}onDestroy(n){return yc(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){yc(this);let e=Ai(this),i=hn(void 0),r;try{return n()}finally{Ai(e),hn(i)}}get(n,e=Wo,i){if(yc(this),n.hasOwnProperty(b0))return n[b0](this);let r=qo(i),o,s=Ai(this),a=hn(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let u=XM(n)&&Sc(n);u&&this.injectableDefInScope(u)?l=qs(Op(n),eu):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Qs():this.parent;return e=r&8&&e===Wo?null:e,c.get(n,e)}catch(c){let l=BM(c);throw l===-200||l===-201?new k(l,null):c}finally{hn(a),Ai(s)}}resolveInjectorInitializers(){let n=te(null),e=Ai(this),i=hn(void 0),r;try{let o=this.get(Xo,Xt,{self:!0});for(let s of o)s()}finally{Ai(e),hn(i),te(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Yt(n);let e=Yo(n)?n:Yt(n&&n.provide),i=ZM(n);if(!Yo(n)&&n.multi===!0){let r=this.records.get(e);r||(r=qs(void 0,eu,!0),r.factory=()=>Np(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=te(null);try{if(e.value===y0)throw Zp("");return e.value===eu&&(e.value=y0,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&KM(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{te(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Yt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Op(t){let n=Sc(t),e=n!==null?n.factory:$r(t);if(e!==null)return e;if(t instanceof y)throw new k(-204,!1);if(t instanceof Function)return YM(t);throw new k(-204,!1)}function YM(t){if(t.length>0)throw new k(-204,!1);let e=PM(t);return e!==null?()=>e.factory(t):()=>new t}function ZM(t){if(F0(t))return qs(void 0,t.useValue);{let n=tg(t);return qs(n,eu)}}function tg(t,n,e){let i;if(Yo(t)){let r=Yt(t);return $r(r)||Op(r)}else if(F0(t))i=()=>Yt(t.useValue);else if(qM(t))i=()=>t.useFactory(...Np(t.deps||[]));else if(WM(t))i=(r,o)=>ne(Yt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Yt(t&&(t.useClass||t.provide));if(QM(t))i=()=>new r(...Np(t.deps));else return $r(r)||Op(r)}return i}function yc(t){if(t.destroyed)throw new k(-205,!1)}function qs(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function QM(t){return!!t.deps}function KM(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function XM(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Pp(t,n){for(let e of t)Array.isArray(e)?Pp(e,n):e&&zp(e)?Pp(e.\u0275providers,n):n(e)}function zt(t,n){let e;t instanceof Zo?(yc(t),e=t):e=new Rp(t);let i,r=Ai(e),o=hn(void 0);try{return n()}finally{Ai(r),hn(o)}}function B0(){return k0()!==void 0||dc()!=null}var gi=0,ee=1,ae=2,Ft=3,qn=4,en=5,Jo=6,Ks=7,Dt=8,Fi=9,_i=10,Le=11,Xs=12,ng=13,Yr=14,pn=15,Zr=16,es=17,Li=18,Bi=19,ig=20,ur=21,hu=22,Gr=23,On=24,ts=25,Vi=26,st=27,V0=1,rg=6,Qr=7,Tc=8,ns=9,ft=10;function fr(t){return Array.isArray(t)&&typeof t[V0]=="object"}function Yn(t){return Array.isArray(t)&&t[V0]===!0}function og(t){return(t.flags&4)!==0}function ji(t){return t.componentOffset>-1}function Js(t){return(t.flags&1)===1}function Hi(t){return!!t.template}function ea(t){return(t[ae]&512)!==0}function is(t){return(t[ae]&256)===256}var sg="svg",j0="math";function Zn(t){for(;Array.isArray(t);)t=t[gi];return t}function ag(t,n){return Zn(n[t])}function Pn(t,n){return Zn(n[t.index])}function pu(t,n){return t.data[n]}function gu(t,n){return t[n]}function cg(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Qn(t,n){let e=n[t];return fr(e)?e:e[gi]}function H0(t){return(t[ae]&4)===4}function _u(t){return(t[ae]&128)===128}function U0(t){return Yn(t[Ft])}function Fn(t,n){return n==null?null:t[n]}function lg(t){t[es]=0}function dg(t){t[ae]&1024||(t[ae]|=1024,_u(t)&&rs(t))}function z0(t,n){for(;t>0;)n=n[Yr],t--;return n}function Ac(t){return!!(t[ae]&9216||t[On]?.dirty)}function vu(t){t[_i].changeDetectionScheduler?.notify(8),t[ae]&64&&(t[ae]|=1024),Ac(t)&&rs(t)}function rs(t){t[_i].changeDetectionScheduler?.notify(0);let n=mr(t);for(;n!==null&&!(n[ae]&8192||(n[ae]|=8192,!_u(n)));)n=mr(n)}function bu(t,n){if(is(t))throw new k(911,!1);t[ur]===null&&(t[ur]=[]),t[ur].push(n)}function $0(t,n){if(t[ur]===null)return;let e=t[ur].indexOf(n);e!==-1&&t[ur].splice(e,1)}function mr(t){let n=t[Ft];return Yn(n)?n[Ft]:n}function ug(t){return t[Ks]??=[]}function mg(t){return t.cleanup??=[]}function G0(t,n,e,i){let r=ug(n);r.push(e),t.firstCreatePass&&mg(t).push(i,r.length-1)}var ve={lFrame:iC(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Fp=!1;function W0(){return ve.lFrame.elementDepthCount}function q0(){ve.lFrame.elementDepthCount++}function fg(){ve.lFrame.elementDepthCount--}function yu(){return ve.bindingsEnabled}function hg(){return ve.skipHydrationRootTNode!==null}function pg(t){return ve.skipHydrationRootTNode===t}function gg(){ve.skipHydrationRootTNode=null}function K(){return ve.lFrame.lView}function ze(){return ve.lFrame.tView}function Ee(t){return ve.lFrame.contextLView=t,t[Dt]}function Se(t){return ve.lFrame.contextLView=null,t}function Tt(){let t=_g();for(;t!==null&&t.type===64;)t=t.parent;return t}function _g(){return ve.lFrame.currentTNode}function Y0(){let t=ve.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function ta(t,n){let e=ve.lFrame;e.currentTNode=t,e.isParent=n}function vg(){return ve.lFrame.isParent}function bg(){ve.lFrame.isParent=!1}function Z0(){return ve.lFrame.contextLView}function yg(){return Fp}function wc(t){let n=Fp;return Fp=t,n}function na(){let t=ve.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Q0(){return ve.lFrame.bindingIndex}function K0(t){return ve.lFrame.bindingIndex=t}function Ui(){return ve.lFrame.bindingIndex++}function Cu(t){let n=ve.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function X0(){return ve.lFrame.inI18n}function J0(t,n){let e=ve.lFrame;e.bindingIndex=e.bindingRootIndex=t,wu(n)}function eC(){return ve.lFrame.currentDirectiveIndex}function wu(t){ve.lFrame.currentDirectiveIndex=t}function tC(t){let n=ve.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function xu(){return ve.lFrame.currentQueryIndex}function Rc(t){ve.lFrame.currentQueryIndex=t}function JM(t){let n=t[ee];return n.type===2?n.declTNode:n.type===1?t[en]:null}function Cg(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=JM(o),r===null||(o=o[Yr],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ve.lFrame=nC();return i.currentTNode=n,i.lView=t,!0}function Du(t){let n=nC(),e=t[ee];ve.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function nC(){let t=ve.lFrame,n=t===null?null:t.child;return n===null?iC(t):n}function iC(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function rC(){let t=ve.lFrame;return ve.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var wg=rC;function Eu(){let t=rC();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function oC(t){return(ve.lFrame.contextLView=z0(t,ve.lFrame.contextLView))[Dt]}function zi(){return ve.lFrame.selectedIndex}function Kr(t){ve.lFrame.selectedIndex=t}function os(){let t=ve.lFrame;return pu(t.tView,t.selectedIndex)}function $t(){ve.lFrame.currentNamespace=sg}function $i(){eT()}function eT(){ve.lFrame.currentNamespace=null}function xg(){return ve.lFrame.currentNamespace}var sC=!0;function Su(){return sC}function Nc(t){sC=t}function Lp(t,n=null,e=null,i){let r=Dg(t,n,e,i);return r.resolveInjectorInitializers(),r}function Dg(t,n=null,e=null,i,r=new Set){let o=[e||Xt,O0(t)],s;return new Zo(o,n||Qs(),s||null,r)}var q=class t{static THROW_IF_NOT_FOUND=Wo;static NULL=new Ys;static create(n,e){if(Array.isArray(n))return Lp({name:""},e,n,"");{let i=n.name??"";return Lp({name:i},n.parent,n.providers,i)}}static \u0275prov=J({token:t,providedIn:"any",factory:()=>ne(kc)});static __NG_ELEMENT_ID__=-1},Z=new y(""),Lt=(()=>{class t{static __NG_ELEMENT_ID__=tT;static __NG_ENV_ID__=e=>e}return t})(),iu=class extends Lt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return is(this._lView)}onDestroy(n){let e=this._lView;return bu(e,n),()=>$0(e,n)}};function tT(){return new iu(K())}var aC=!1,cC=new y(""),hr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Pt(!1);debugTaskTracker=d(cC,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new le(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=J({token:t,providedIn:"root",factory:()=>new t})}return t})(),Bp=class extends S{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,B0()&&(this.destroyRef=d(Lt,{optional:!0})??void 0,this.pendingTasks=d(hr,{optional:!0})??void 0)}emit(n){let e=te(null);try{super.next(n)}finally{te(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof he&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},U=Bp;function ru(...t){}function Eg(t){let n,e;function i(){t=ru;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function lC(t){return queueMicrotask(()=>t()),()=>{t=ru}}var Sg="isAngularZone",xc=Sg+"_ID",nT=0,B=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new U(!1);onMicrotaskEmpty=new U(!1);onStable=new U(!1);onError=new U(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=aC}=n;if(typeof Zone>"u")throw new k(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,oT(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Sg)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new k(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new k(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,iT,ru,ru);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},iT={};function Ig(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function rT(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Eg(()=>{t.callbackScheduled=!1,Vp(t),t.isCheckStableRunning=!0,Ig(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Vp(t)}function oT(t){let n=()=>{rT(t)},e=nT++;t._inner=t._inner.fork({name:"angular",properties:{[Sg]:!0,[xc]:e,[xc+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(sT(c))return i.invokeTask(o,s,a,c);try{return C0(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),w0(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return C0(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!aT(c)&&n(),w0(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Vp(t),Ig(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Vp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function C0(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function w0(t){t._nesting--,Ig(t)}var Dc=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new U;onMicrotaskEmpty=new U;onStable=new U;onError=new U;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function sT(t){return dC(t,"__ignore_ng_zone__")}function aT(t){return dC(t,"__scheduler_tick__")}function dC(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Jt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Ln=new y("",{factory:()=>{let t=d(B),n=d(Ue),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Jt),e.handleError(i))})}}}),uC={provide:Xo,useValue:()=>{let t=d(Jt,{optional:!0})},multi:!0},cT=new y("",{factory:()=>{let t=d(Z).defaultView;if(!t)return;let n=d(Ln),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(Lt).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function kg(){return pi([N0(()=>{d(cT)})])}function V(t,n){let[e,i,r]=mp(t,n?.equal),o=e,s=o[wt];return o.set=i,o.update=r,o.asReadonly=Iu.bind(o),o}function Iu(){let t=this[wt];if(t.readonlyFn===void 0){let n=()=>this();n[wt]=t,t.readonlyFn=n}return t.readonlyFn}var pr=new y("",{factory:()=>lT}),lT="ng";var ku=new y(""),ss=new y("",{providedIn:"platform",factory:()=>"unknown"}),Oc=new y(""),Xr=new y("",{factory:()=>d(Z).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var ia=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=dT}return t})();function dT(){return new ia(K(),Tt())}var Oi=class{},Pc=new y("",{factory:()=>!0});var Mg=new y(""),Mu=(()=>{class t{static \u0275prov=J({token:t,providedIn:"root",factory:()=>new jp})}return t})(),jp=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},ou=class{[wt];constructor(n){this[wt]=n}destroy(){this[wt].destroy()}};function Kn(t,n){let e=n?.injector??d(q),i=n?.manualCleanup!==!0?e.get(Lt):null,r,o=e.get(ia,null,{optional:!0}),s=e.get(Oi);return o!==null?(r=fT(o.view,s,t),i instanceof iu&&i._lView===o.view&&(i=null)):r=hT(t,e.get(Mu),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new ou(r)}var mC=Q(b({},fp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=wc(!1);try{hp(this)}finally{wc(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=te(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],te(t)}}}),uT=Q(b({},mC),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(jr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),mT=Q(b({},mC),{consumerMarkedDirty(){this.view[ae]|=8192,rs(this.view),this.notifier.notify(13)},destroy(){if(jr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Gr]?.delete(this)}});function fT(t,n,e){let i=Object.create(mT);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=fC(i,e),t[Gr]??=new Set,t[Gr].add(i),i.consumerMarkedDirty(i),i}function hT(t,n,e){let i=Object.create(uT);return i.fn=fC(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function fC(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Dn(t){return typeof t=="function"&&t[wt]!==void 0}function Tu(t){return Dn(t)&&typeof t.set=="function"}var Fc=(()=>{class t{internalPendingTasks=d(hr);scheduler=d(Oi);errorHandler=d(Ln);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=J({token:t,providedIn:"root",factory:()=>new t})}return t})();function qc(t){return{toString:t}.toString()}var Re=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Re||{}),Vu=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function JC(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var ew=null,$e=(()=>{ew=hC;let t=()=>hC;return t.ngInherit=!0,t})();function xT(){return ew}function hC(t){return t.type.prototype.ngOnChanges&&(t.setInput=ET),DT}function DT(){let t=tw(this),n=t?.current;if(n){let e=t.previous;if(e===qr)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function ET(t,n,e,i,r){let o=this.declaredInputs[i],s=tw(t)||ST(t,{previous:qr,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Vu(l&&l.currentValue,e,c===qr),JC(t,n,r,e)}var Hg="__ngSimpleChanges__";function tw(t){return Object.hasOwn(t,Hg)&&t[Hg]||null}function ST(t,n){return t[Hg]=n}var pC=[];var Be=function(t,n=null,e){for(let i=0;i<pC.length;i++){let r=pC[i];r(t,n,e)}};function IT(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=xT()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function nw(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Pu(t,n,e){iw(t,n,3,e)}function Fu(t,n,e,i){(t[ae]&3)===e&&iw(t,n,e,i)}function Tg(t,n){let e=t[ae];(e&3)===n&&(e&=16383,e+=1,t[ae]=e)}function iw(t,n,e,i){let r=i!==void 0?t[es]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[es]+=65536),(a<o||o==-1)&&(kT(t,e,n,c),t[es]=(t[es]&4294901760)+c+2),c++}function gC(t,n){Be(Re.LifecycleHookStart,t,n);let e=te(null);try{n.call(t)}finally{te(e),Be(Re.LifecycleHookEnd,t,n)}}function kT(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[ae]>>14<t[es]>>16&&(t[ae]&3)===n&&(t[ae]+=16384,gC(a,o)):gC(a,o)}var oa=-1,cs=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function MT(t){return(t.flags&8)!==0}function TT(t){return(t.flags&16)!==0}function AT(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];RT(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function rw(t){return t===3||t===4||t===6}function RT(t){return t.charCodeAt(0)===64}function sa(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?_C(t,e,r,null,n[++i]):_C(t,e,r,null,null))}}return t}function _C(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function ow(t){return t!==oa}function ju(t){return t&32767}function NT(t){return t>>16}function Hu(t,n){let e=NT(t),i=n;for(;e>0;)i=i[Yr],e--;return i}var Ug=!0;function Uu(t){let n=Ug;return Ug=t,n}var OT=256,sw=OT-1,aw=5,PT=0,Gi={};function FT(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Qo)&&(i=e[Qo]),i==null&&(i=e[Qo]=PT++);let r=i&sw,o=1<<r;n.data[t+(r>>aw)]|=o}function zu(t,n){let e=cw(t,n);if(e!==-1)return e;let i=n[ee];i.firstCreatePass&&(t.injectorIndex=n.length,Ag(i.data,t),Ag(n,null),Ag(i.blueprint,null));let r=w_(t,n),o=t.injectorIndex;if(ow(r)){let s=ju(r),a=Hu(r,n),c=a[ee].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function Ag(t,n){t.push(0,0,0,0,0,0,0,0,n)}function cw(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function w_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=fw(r),i===null)return oa;if(e++,r=r[Yr],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return oa}function zg(t,n,e){FT(t,n,e)}function LT(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(rw(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function lw(t,n,e){if(e&8||t!==void 0)return t;du(n,"NodeInjector")}function dw(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Fi],o=hn(void 0);try{return r?r.get(n,i,e&8):Qp(n,i,e&8)}finally{hn(o)}}return lw(i,n,e)}function uw(t,n,e,i=0,r){if(t!==null){if(n[ae]&2048&&!(i&2)){let s=HT(t,n,e,i,Gi);if(s!==Gi)return s}let o=mw(t,n,e,i,Gi);if(o!==Gi)return o}return dw(n,e,i,r)}function mw(t,n,e,i,r){let o=VT(e);if(typeof o=="function"){if(!Cg(n,t,i))return i&1?lw(r,e,i):dw(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))du(e);else return s}finally{wg()}}else if(typeof o=="number"){let s=null,a=cw(t,n),c=oa,l=i&1?n[pn][en]:null;for((a===-1||i&4)&&(c=a===-1?w_(t,n):n[a+8],c===oa||!bC(i,!1)?a=-1:(s=n[ee],a=ju(c),n=Hu(c,n)));a!==-1;){let u=n[ee];if(vC(o,a,u.data)){let m=BT(a,n,e,s,i,l);if(m!==Gi)return m}c=n[a+8],c!==oa&&bC(i,n[ee].data[a+8]===l)&&vC(o,a,n)?(s=u,a=ju(c),n=Hu(c,n)):a=-1}}return r}function BT(t,n,e,i,r,o){let s=n[ee],a=s.data[t+8],c=i==null?ji(a)&&Ug:i!=s&&(a.type&3)!==0,l=r&1&&o===a,u=Lu(a,s,e,c,l);return u!==null?Hc(n,s,u,a,r):Gi}function Lu(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,u=o>>20,m=i?a:a+u,g=r?a+u:l;for(let _=m;_<g;_++){let C=s[_];if(_<c&&e===C||_>=c&&C.type===e)return _}if(r){let _=s[c];if(_&&Hi(_)&&_.type===e)return c}return null}function Hc(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof cs){let a=o;if(a.resolving)throw Zp("");let c=Uu(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],u,m=a.injectImpl?hn(a.injectImpl):null,g=Cg(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&IT(e,s[e],n)}finally{m!==null&&hn(m),Uu(c),a.resolving=!1,wg()}}return o}function VT(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Qo)?t[Qo]:void 0;return typeof n=="number"?n>=0?n&sw:jT:n}function vC(t,n,e){let i=1<<t;return!!(e[n+(t>>aw)]&i)}function bC(t,n){return!(t&2)&&!(t&1&&n)}var Jr=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return uw(this._tNode,this._lView,n,qo(i),e)}};function jT(){return new Jr(Tt(),K())}function _t(t){return qc(()=>{let n=t.prototype.constructor,e=n[Cc]||$g(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Cc]||$g(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function $g(t){return Hp(t)?()=>{let n=$g(Yt(t));return n&&n()}:$r(t)}function HT(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[ae]&2048&&!ea(s);){let a=mw(o,s,e,i|2,Gi);if(a!==Gi)return a;let c=o.parent;if(!c){let l=s[ig];if(l){let u=l.get(e,Gi,i&-5);if(u!==Gi)return u}c=fw(s),s=s[Yr]}o=c}return r}function fw(t){let n=t[ee],e=n.type;return e===2?n.declTNode:e===1?t[en]:null}function Yc(t){return LT(Tt(),t)}function N(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function UT(){return ma(Tt(),K())}function ma(t,n){return new F(Pn(t,n))}var F=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=UT}return t})();function hw(t){return t instanceof F?t.nativeElement:t}function zT(){return this._results[Symbol.iterator]()}var En=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new S}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=T0(n);(this._changesDetected=!M0(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=zT};function pw(t){return(t.flags&128)===128}var x_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(x_||{}),gw=new Map,$T=0;function GT(){return $T++}function WT(t){gw.set(t[Bi],t)}function Gg(t){gw.delete(t[Bi])}var yC="__ngContext__";function aa(t,n){fr(n)?(t[yC]=n[Bi],WT(n)):t[yC]=n}function _w(t){return bw(t[Xs])}function vw(t){return bw(t[qn])}function bw(t){for(;t!==null&&!Yn(t);)t=t[qn];return t}var Wg;function D_(t){Wg=t}function yw(){if(Wg!==void 0)return Wg;if(typeof document<"u")return document;throw new k(210,!1)}var Cw="r";var ww="di";var xw=!1,Dw=new y("",{factory:()=>xw});var CC=new WeakMap;function qT(t,n){if(t==null||typeof t!="object")return;let e=CC.get(t);e||(e=new WeakSet,CC.set(t,e)),e.add(n)}var YT=(t,n,e,i)=>{};function ZT(t,n,e,i){YT(t,n,e,i)}function im(t){return(t.flags&32)===32}var QT=()=>null;function Ew(t,n,e=!1){return QT(t,n,e)}function Sw(t,n){let e=t.contentQueries;if(e!==null){let i=te(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];Rc(o),a.contentQueries(2,n[s],s)}}}finally{te(i)}}}function qg(t,n,e){Rc(0);let i=te(null);try{n(t,e)}finally{te(i)}}function E_(t,n,e){if(og(n)){let i=te(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{te(i)}}}var yi=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(yi||{});var Au;function KT(){if(Au===void 0&&(Au=null,Wn.trustedTypes))try{Au=Wn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Au}function rm(t){return KT()?.createHTML(t)||t}var Ru;function XT(){if(Ru===void 0&&(Ru=null,Wn.trustedTypes))try{Ru=Wn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Ru}function wC(t){return XT()?.createScriptURL(t)||t}var gr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${su})`}},Yg=class extends gr{getTypeName(){return"HTML"}},Zg=class extends gr{getTypeName(){return"Style"}},Qg=class extends gr{getTypeName(){return"Script"}},Kg=class extends gr{getTypeName(){return"URL"}},Xg=class extends gr{getTypeName(){return"ResourceURL"}};function Ci(t){return t instanceof gr?t.changingThisBreaksApplicationSecurity:t}function _r(t,n){let e=Iw(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${su})`)}return e===n}function Iw(t){return t instanceof gr&&t.getTypeName()||null}function S_(t){return new Yg(t)}function I_(t){return new Zg(t)}function k_(t){return new Qg(t)}function M_(t){return new Kg(t)}function T_(t){return new Xg(t)}function JT(t){let n=new e_(t);return eA()?new Jg(n):n}var Jg=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(rm(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},e_=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=rm(n),e}};function eA(){try{return!!new window.DOMParser().parseFromString(rm(""),"text/html")}catch{return!1}}var tA=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Zc(t){return t=String(t),t.match(tA)?t:"unsafe:"+t}function vr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Qc(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var kw=vr("area,br,col,hr,img,wbr"),Mw=vr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Tw=vr("rp,rt"),nA=Qc(Tw,Mw),iA=Qc(Mw,vr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),rA=Qc(Tw,vr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),xC=Qc(kw,iA,rA,nA),Aw=vr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),oA=vr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),sA=vr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),aA=Qc(Aw,oA,sA),cA=vr("script,style,template"),t_=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=uA(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=dA(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=DC(n).toLowerCase();if(!xC.hasOwnProperty(e))return this.sanitizedSomething=!0,!cA.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!aA.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;Aw[a]&&(c=Zc(c)),this.buf.push(" ",s,'="',EC(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=DC(n).toLowerCase();xC.hasOwnProperty(e)&&!kw.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(EC(n))}};function lA(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function dA(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Rw(n);return n}function uA(t){let n=t.firstChild;if(n&&lA(t,n))throw Rw(n);return n}function DC(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Rw(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var mA=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,fA=/([^\#-~ |!])/g;function EC(t){return t.replace(/&/g,"&amp;").replace(mA,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(fA,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Nu;function A_(t,n){let e=null;try{Nu=Nu||JT(t);let i=n?String(n):"";e=Nu.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Nu.getInertBodyElement(i)}while(i!==o);let a=new t_().sanitizeChildren(SC(e)||e);return rm(a)}finally{if(e){let i=SC(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function SC(t){return"content"in t&&hA(t)?t.content:null}function hA(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var pA=/^>|^->|<!--|-->|--!>|<!-$/g,gA=/(<|>)/g,_A="\u200B$1\u200B";function vA(t){return t.replace(pA,n=>n.replace(gA,_A))}function bA(t,n){return t.createText(n)}function yA(t,n,e){t.setValue(n,e)}function CA(t,n){return t.createComment(vA(n))}function Nw(t,n,e){return t.createElement(n,e)}function $u(t,n,e,i,r){t.insertBefore(n,e,i,r)}function Ow(t,n,e){t.appendChild(n,e)}function IC(t,n,e,i,r){i!==null?$u(t,n,e,i,r):Ow(t,n,e)}function Pw(t,n,e,i){t.removeChild(null,n,e,i)}function wA(t,n,e){t.setAttribute(n,"style",e)}function xA(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function Fw(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&AT(t,n,i),r!==null&&xA(t,n,r),o!==null&&wA(t,n,o)}var Bt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Bt||{});function Xn(t){let n=Bw();return n?n.sanitize(Bt.URL,t)||"":_r(t,"URL")?Ci(t):Zc(Ko(t))}function Lw(t){let n=Bw();if(n)return wC(n.sanitize(Bt.RESOURCE_URL,t)||"");if(_r(t,"ResourceURL"))return wC(Ci(t));throw new k(904,!1)}var DA={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function EA(t,n){return DA[t.toLowerCase()]?.[n.toLowerCase()]===!0?Lw:Xn}function R_(t,n,e){return EA(n,e)(t)}function Bw(){let t=K();return t&&t[_i].sanitizer}function SA(t){return t instanceof Function?t():t}function IA(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var Vw="ng-template";function kA(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&IA(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(N_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function N_(t){return t.type===4&&t.value!==Vw}function MA(t,n,e){let i=t.type===4&&!e?Vw:t.value;return n===i}function TA(t,n,e){let i=4,r=t.attrs,o=r!==null?NA(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!vi(i)&&!vi(c))return!1;if(s&&vi(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!MA(t,c,e)||c===""&&n.length===1){if(vi(i))return!1;s=!0}}else if(i&8){if(r===null||!kA(t,r,c,e)){if(vi(i))return!1;s=!0}}else{let l=n[++a],u=AA(c,r,N_(t),e);if(u===-1){if(vi(i))return!1;s=!0;continue}if(l!==""){let m;if(u>o?m="":m=r[u+1].toLowerCase(),i&2&&l!==m){if(vi(i))return!1;s=!0}}}}return vi(i)||s}function vi(t){return(t&1)===0}function AA(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return OA(n,t)}function jw(t,n,e=!1){for(let i=0;i<n.length;i++)if(TA(t,n[i],e))return!0;return!1}function RA(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function NA(t){for(let n=0;n<t.length;n++){let e=t[n];if(rw(e))return n}return t.length}function OA(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function PA(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function kC(t,n){return t?":not("+n.trim()+")":n}function FA(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!vi(s)&&(n+=kC(o,r),r=""),i=s,o=o||!vi(i);e++}return r!==""&&(n+=kC(o,r)),n}function LA(t){return t.map(FA).join(",")}function BA(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!vi(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var tn={},Wi=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Wi||{}),VA;function O_(t,n){return VA(t,n)}var ZW=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var n_=new WeakMap;function Hw(t){return t?t[Yr]??t:null}var Bc=new WeakSet;function jA(t,n,e){let i=n_.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=Hw(e);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],u=c.parentNode;c===n?(i.splice(a,1),Bc.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):u&&r&&u!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function HA(t,n,e){let i=Hw(e),r=n_.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):n_.set(t,[{el:n,declarationView:i}])}var eo=new Set,om=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(om||{}),Yi=new y(""),MC=new Set;function br(t){MC.has(t)||(MC.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var sm=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=J({token:t,providedIn:"root",factory:()=>new t})}return t})(),P_=[0,1,2,3],F_=(()=>{class t{ngZone=d(B);scheduler=d(Oi);errorHandler=d(Jt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Yi,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Be(Re.AfterRenderHooksStart),this.executing=!0;for(let i of P_)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Be(Re.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[ts]??=[]).push(e),rs(i),i[ae]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(om.AFTER_NEXT_RENDER,e):e()}static \u0275prov=J({token:t,providedIn:"root",factory:()=>new t})}return t})(),Uc=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[ts];n&&(this.view[ts]=n.filter(e=>e!==this))}};function vt(t,n){let e=n?.injector??d(q);return br("NgAfterNextRender"),zA(t,e,n,!0)}function UA(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function zA(t,n,e,i){let r=n.get(sm);r.impl??=n.get(F_);let o=n.get(Yi,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(Lt):null,a=n.get(ia,null,{optional:!0}),c=new Uc(r.impl,UA(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var L_=new y("",{factory:()=>{let t=d(Ue),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function Uw(t,n,e){let i=t.get(L_);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function $A(t,n){let e=t.get(L_);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function GA(t,n){let e=t.get(L_);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function WA(t,n){for(let[e,i]of n)Uw(t,i.animateFns)}function TC(t,n,e,i){let r=t?.[Vi]?.enter;n!==null&&r&&r.has(e.index)&&WA(i,r)}function AC(t,n,e,i){try{e.get(kc)}catch{return i(!1)}let r=t?.[Vi];r?.enter?.has(n.index)&&$A(e,r.enter.get(n.index).animateFns);let o=qA(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];am(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&eo.add(t[Bi]),Uw(e,()=>YA(t,n,r||void 0,o,i),r||void 0)}function qA(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let c=t[ee].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function YA(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(t&&am(t,n,o),o.length>0){let s=e||t?.[Vi];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),QA(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&eo.delete(t[Bi]),r(!0)})}else t&&eo.delete(t[Bi]),r(!1)}function am(t,n,e){if(n.type&12){let r=t[n.index];if(Yn(r))for(let o=ft;o<r.length;o++){let s=r[o];s[ee].type===2&&ZA(s,e)}}let i=n.child;for(;i;)am(t,i,e),i=i.next}function ZA(t,n){let e=t[Vi];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[ee].firstChild;for(;i;)am(t,i,n),i=i.next}function QA(t,n,e){n.then(()=>{t[Vi]?.running===n&&(t[Vi].running=void 0,eo.delete(t[Bi])),e(!0)})}function ra(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;Yn(r)?c=r:fr(r)&&(l=!0,r=r[gi]);let u=Zn(r);t===0&&i!==null?(TC(a,i,o,e),s==null?Ow(n,i,u):$u(n,i,u,s||null,!0)):t===1&&i!==null?(TC(a,i,o,e),$u(n,i,u,s||null,!0),jA(o,u,a)):t===2?(a?.[Vi]?.leave?.has(o.index)&&HA(o,u,a),Bc.delete(u),AC(a,o,e,m=>{if(Bc.has(u)){Bc.delete(u);return}Pw(n,u,l,m)})):t===3&&(Bc.delete(u),AC(a,o,e,()=>{n.destroyNode(u)})),c!=null&&sR(n,t,e,c,o,i,s)}}function KA(t,n){zw(t,n),n[gi]=null,n[en]=null}function XA(t,n,e,i,r,o){i[gi]=r,i[en]=n,lm(t,i,e,1,r,o)}function zw(t,n){n[_i].changeDetectionScheduler?.notify(9),lm(t,n,n[Le],2,null,null)}function JA(t){let n=t[Xs];if(!n)return Rg(t[ee],t);for(;n;){let e=null;if(fr(n))e=n[Xs];else{let i=n[ft];i&&(e=i)}if(!e){for(;n&&!n[qn]&&n!==t;)fr(n)&&Rg(n[ee],n),n=n[Ft];n===null&&(n=t),fr(n)&&Rg(n[ee],n),e=n&&n[qn]}n=e}}function B_(t,n){let e=t[ns],i=e.indexOf(n);e.splice(i,1)}function cm(t,n){if(is(n))return;let e=n[Le];e.destroyNode&&lm(t,n,e,3,null,null),JA(n)}function Rg(t,n){if(is(n))return;let e=te(null);try{n[ae]&=-129,n[ae]|=256,n[On]&&jr(n[On]),tR(t,n),eR(t,n),n[ee].type===1&&n[Le].destroy();let i=n[Zr];if(i!==null&&Yn(n[Ft])){i!==n[Ft]&&B_(i,n);let r=n[Li];r!==null&&r.detachView(t)}Gg(n)}finally{te(e)}}function eR(t,n){let e=t.cleanup,i=n[Ks];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[Ks]=null);let r=n[ur];if(r!==null){n[ur]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Gr];if(o!==null){n[Gr]=null;for(let s of o)s.destroy()}}function tR(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof cs)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];Be(Re.LifecycleHookStart,a,c);try{c.call(a)}finally{Be(Re.LifecycleHookEnd,a,c)}}else{Be(Re.LifecycleHookStart,r,o);try{o.call(r)}finally{Be(Re.LifecycleHookEnd,r,o)}}}}}function $w(t,n,e){return nR(t,n.parent,e)}function nR(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[gi];if(ji(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===yi.None||r===yi.Emulated)return null}return Pn(i,e)}function Gw(t,n,e){return rR(t,n,e)}function iR(t,n,e){return t.type&40?Pn(t,e):null}var rR=iR,RC;function V_(t,n,e,i){let r=$w(t,i,n),o=n[Le],s=i.parent||n[en],a=Gw(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)IC(o,r,e[c],a,!1);else IC(o,r,e,a,!1);RC!==void 0&&RC(o,i,n,e,r)}function Vc(t,n){if(n!==null){let e=n.type;if(e&3)return Pn(n,t);if(e&4)return i_(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Vc(t,i);{let r=t[n.index];return Yn(r)?i_(-1,r):Zn(r)}}else{if(e&128)return Vc(t,n.next);if(e&32)return O_(n,t)()||Zn(t[n.index]);{let i=Ww(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=mr(t[pn]);return Vc(r,i)}else return Vc(t,n.next)}}}return null}function Ww(t,n){if(n!==null){let i=t[pn][en],r=n.projection;return i.projection[r]}return null}function i_(t,n){let e=ft+t+1;if(e<n.length){let i=n[e],r=i[ee].firstChild;if(r!==null)return Vc(i,r)}return n[Qr]}function j_(t,n,e,i,r,o,s){for(;e!=null;){let a=i[Fi];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&aa(Zn(c),i),e.flags|=2),!im(e))if(l&8)j_(t,n,e.child,i,r,o,!1),ra(n,t,a,r,c,e,o,i);else if(l&32){let u=O_(e,i),m;for(;m=u();)ra(n,t,a,r,m,e,o,i);ra(n,t,a,r,c,e,o,i)}else l&16?qw(t,n,i,e,r,o):ra(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function lm(t,n,e,i,r,o){j_(e,i,t.firstChild,n,r,o,!1)}function oR(t,n,e){let i=n[Le],r=$w(t,e,n),o=e.parent||n[en],s=Gw(o,e,n);qw(i,0,n,e,r,s)}function qw(t,n,e,i,r,o){let s=e[pn],c=s[en].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];ra(n,t,e[Fi],r,u,i,o,e)}else{let l=c,u=s[Ft];pw(i)&&(l.flags|=128),j_(t,n,l,u,r,o,!0)}}function sR(t,n,e,i,r,o,s){let a=i[Qr],c=Zn(i);a!==c&&ra(n,t,e,o,a,r,s);for(let l=ft;l<i.length;l++){let u=i[l];lm(u[ee],u,t,n,o,a)}}function aR(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Wi.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Wi.Important),t.setStyle(e,i,r,o))}}function H_(t,n,e,i,r,o,s,a,c,l,u){let m=st+i,g=m+r,_=cR(m,g),C=typeof l=="function"?l():l;return _[ee]={type:t,blueprint:_,template:e,queries:null,viewQuery:a,declTNode:n,data:_.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:C,incompleteFirstPass:!1,ssrId:u}}function cR(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:tn);return e}function lR(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=H_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function U_(t,n,e,i,r,o,s,a,c,l,u){let m=n.blueprint.slice();return m[gi]=r,m[ae]=i|4|128|8|64|1024,(l!==null||t&&t[ae]&2048)&&(m[ae]|=2048),lg(m),m[Ft]=m[Yr]=t,m[Dt]=e,m[_i]=s||t&&t[_i],m[Le]=a||t&&t[Le],m[Fi]=c||t&&t[Fi]||null,m[en]=o,m[Bi]=GT(),m[Jo]=u,m[ig]=l,m[pn]=n.type==2?t[pn]:m,m}function dR(t,n,e){let i=Pn(n,t),r=lR(e),o=t[_i].rendererFactory,s=z_(t,U_(t,r,null,Yw(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function Yw(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Zw(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function z_(t,n){return t[Xs]?t[ng][qn]=n:t[Xs]=n,t[ng]=n,n}function p(t=1){Qw(ze(),K(),zi()+t,!1)}function Qw(t,n,e,i){if(!i)if((n[ae]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Pu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Fu(n,o,0,e)}Kr(e)}var dm=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(dm||{});function ls(t,n,e,i){let r=te(null);try{let[o,s,a]=t.inputs[e],c=null;(s&dm.SignalBased)!==0&&(c=n[o][wt]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):JC(n,c,o,i)}finally{te(r)}}function Kw(t,n,e,i,r){let o=zi(),s=i&2;try{Kr(-1),s&&n.length>st&&Qw(t,n,st,!1);let a=s?Re.TemplateUpdateStart:Re.TemplateCreateStart;Be(a,r,e),e(i,r)}finally{Kr(o);let a=s?Re.TemplateUpdateEnd:Re.TemplateCreateEnd;Be(a,r,e)}}function um(t,n,e){pR(t,n,e),(e.flags&64)===64&&gR(t,n,e)}function Kc(t,n,e=Pn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function uR(t,n,e,i){let o=i.get(Dw,xw)||e===yi.ShadowDom||e===yi.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return mR(s),s}function mR(t){fR(t)}var fR=()=>null;function hR(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function Xw(t,n,e,i,r,o){let s=n[ee];if(mm(t,s,n,e,i)){ji(t)&&ex(n,t.index);return}t.type&3&&(e=hR(e)),Jw(t,n,e,i,r,o)}function Jw(t,n,e,i,r,o){if(t.type&3){let s=Pn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function ex(t,n){let e=Qn(n,t);e[ae]&16||(e[ae]|=64)}function pR(t,n,e){let i=e.directiveStart,r=e.directiveEnd;ji(e)&&dR(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||zu(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=Hc(n,t,s,e);if(aa(c,n),o!==null&&bR(n,s-i,c,a,e,o),Hi(a)){let l=Qn(e.index,n);l[Dt]=Hc(n,t,s,e)}}}function gR(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=eC();try{Kr(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];wu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&_R(c,l)}}finally{Kr(-1),wu(s)}}function _R(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function $_(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];jw(n,o.selectors,!1)&&(i??=[],Hi(o)?i.unshift(o):i.push(o))}return i}function vR(t,n,e,i,r,o){let s=Pn(t,n);tx(n[Le],s,o,t.value,e,i,r)}function tx(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Ko(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function bR(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];ls(i,e,c,l)}}function G_(t,n,e,i,r){let o=st+e,s=n[ee],a=r(s,n,t,i,e);n[o]=a,ta(t,!0);let c=t.type===2;return c?(Fw(n[Le],a,t),(W0()===0||Js(t))&&aa(a,n),q0()):aa(a,n),Su()&&(!c||!im(t))&&V_(s,n,a,t),t}function W_(t){let n=t;return vg()?bg():(n=n.parent,ta(n,!1)),n}function yR(t,n){let e=t[Fi];if(!e)return;let i;try{i=e.get(Ln,null)}catch{i=null}i?.(n)}function mm(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],u=s[c+1],m=n.data[l];ls(m,e[l],u,r),a=!0}if(o)for(let c of o){let l=e[c],u=n.data[c];ls(u,l,i,r),a=!0}return a}function CR(t,n,e,i,r,o){let s=null,a=null,c=null,l=!1,u=t.directiveToIndex.get(i.type);if(typeof u=="number"?s=u:[s,a,c]=u,a!==null&&c!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let m=t.hostDirectiveInputs[r];for(let g=0;g<m.length;g+=2){let _=m[g];if(_>=a&&_<=c){let C=n.data[_],I=m[g+1];ls(C,e[_],I,o),l=!0}else if(_>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(ls(i,e[s],r,o),l=!0),l}function wR(t,n){let e=Qn(n,t),i=e[ee];xR(i,e);let r=e[gi];r!==null&&e[Jo]===null&&(e[Jo]=Ew(r,e[Fi])),Be(Re.ComponentStart);try{q_(i,e,e[Dt])}finally{Be(Re.ComponentEnd,e[Dt])}}function xR(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function q_(t,n,e){Du(n);try{let i=t.viewQuery;i!==null&&qg(1,i,e);let r=t.template;r!==null&&Kw(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Li]?.finishViewCreation(t),t.staticContentQueries&&Sw(t,n),t.staticViewQueries&&qg(2,t.viewQuery,e);let o=t.components;o!==null&&DR(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[ae]&=-5,Eu()}}function DR(t,n){for(let e=0;e<n.length;e++)wR(t,n[e])}function Xc(t,n,e,i){let r=te(null);try{let o=n.tView,a=t[ae]&4096?4096:16,c=U_(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[Zr]=l;let u=t[Li];return u!==null&&(c[Li]=u.createEmbeddedView(o)),q_(o,c,e),c}finally{te(r)}}function ca(t,n){return!n||n.firstChild===null||pw(t)}function zc(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Zn(o)),Yn(o)&&nx(o,i);let s=e.type;if(s&8)zc(t,n,e.child,i);else if(s&32){let a=O_(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=Ww(n,e);if(Array.isArray(a))i.push(...a);else{let c=mr(n[pn]);zc(c[ee],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function nx(t,n){for(let e=ft;e<t.length;e++){let i=t[e],r=i[ee].firstChild;r!==null&&zc(i[ee],i,r,n)}t[Qr]!==t[gi]&&n.push(t[Qr])}function ix(t){if(t[ts]!==null){for(let n of t[ts])n.impl.addSequence(n);t[ts].length=0}}var rx=[];function ER(t){return t[On]??SR(t)}function SR(t){let n=rx.pop()??Object.create(kR);return n.lView=t,n}function IR(t){t.lView[On]!==t&&(t.lView=null,rx.push(t))}var kR=Q(b({},Lr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{rs(t.lView)},consumerOnSignalRead(){this.lView[On]=this}});function MR(t){let n=t[On]??Object.create(TR);return n.lView=t,n}var TR=Q(b({},Lr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=mr(t.lView);for(;n&&!ox(n[ee]);)n=mr(n);n&&dg(n)},consumerOnSignalRead(){this.lView[On]=this}});function ox(t){return t.type!==2}function sx(t){if(t[Gr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Gr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[ae]&8192)}}var AR=100;function ax(t,n=0){let i=t[_i].rendererFactory,r=!1;r||i.begin?.();try{RR(t,n)}finally{r||i.end?.()}}function RR(t,n){let e=yg();try{wc(!0),r_(t,n);let i=0;for(;Ac(t);){if(i===AR)throw new k(103,!1);i++,r_(t,1)}}finally{wc(e)}}function NR(t,n,e,i){if(is(n))return;let r=n[ae],o=!1,s=!1;Du(n);let a=!0,c=null,l=null;o||(ox(t)?(l=ER(n),c=lr(l)):Cd()===null?(a=!1,l=MR(n),c=lr(l)):n[On]&&(jr(n[On]),n[On]=null));try{lg(n),K0(t.bindingStartIndex),e!==null&&Kw(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let _=t.preOrderCheckHooks;_!==null&&Pu(n,_,null)}else{let _=t.preOrderHooks;_!==null&&Fu(n,_,0,null),Tg(n,0)}if(s||OR(n),sx(n),cx(n,0),t.contentQueries!==null&&Sw(t,n),!o)if(u){let _=t.contentCheckHooks;_!==null&&Pu(n,_)}else{let _=t.contentHooks;_!==null&&Fu(n,_,1),Tg(n,1)}FR(t,n);let m=t.components;m!==null&&dx(n,m,0);let g=t.viewQuery;if(g!==null&&qg(2,g,i),!o)if(u){let _=t.viewCheckHooks;_!==null&&Pu(n,_)}else{let _=t.viewHooks;_!==null&&Fu(n,_,2),Tg(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[hu]){for(let _ of n[hu])_();n[hu]=null}o||(ix(n),n[ae]&=-73)}catch(u){throw o||rs(n),u}finally{l!==null&&(Vr(l,c),a&&IR(l)),Eu()}}function cx(t,n){for(let e=_w(t);e!==null;e=vw(e))for(let i=ft;i<e.length;i++){let r=e[i];lx(r,n)}}function OR(t){for(let n=_w(t);n!==null;n=vw(n)){if(!(n[ae]&2))continue;let e=n[ns];for(let i=0;i<e.length;i++){let r=e[i];dg(r)}}}function PR(t,n,e){Be(Re.ComponentStart);let i=Qn(n,t);try{lx(i,e)}finally{Be(Re.ComponentEnd,i[Dt])}}function lx(t,n){_u(t)&&r_(t,n)}function r_(t,n){let i=t[ee],r=t[ae],o=t[On],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Vs(o)),s||=!1,o&&(o.dirty=!1),t[ae]&=-9217,s)NR(i,t,i.template,t[Dt]);else if(r&8192){let a=te(null);try{sx(t),cx(t,1);let c=i.components;c!==null&&dx(t,c,1),ix(t)}finally{te(a)}}}function dx(t,n,e){for(let i=0;i<n.length;i++)PR(t,n[i],e)}function FR(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Kr(~r);else{let o=r,s=e[++i],a=e[++i];J0(s,o);let c=n[o];Be(Re.HostBindingsUpdateStart,c);try{a(2,c)}finally{Be(Re.HostBindingsUpdateEnd,c)}}}}finally{Kr(-1)}}function Y_(t,n){let e=yg()?64:1088;for(t[_i].changeDetectionScheduler?.notify(n);t;){t[ae]|=e;let i=mr(t);if(ea(t)&&!i)return t;t=i}return null}function ux(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function mx(t,n){let e=ft+n;if(e<t.length)return t[e]}function Jc(t,n,e,i=!0){let r=n[ee];if(LR(r,n,t,e),i){let s=i_(e,t),a=n[Le],c=a.parentNode(t[Qr]);c!==null&&XA(r,t[en],a,n,c,s)}let o=n[Jo];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function fx(t,n){let e=$c(t,n);return e!==void 0&&cm(e[ee],e),e}function $c(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e];if(i){let r=i[Zr];r!==null&&r!==t&&B_(r,i),n>0&&(t[e-1][qn]=i[qn]);let o=Ic(t,ft+n);KA(i[ee],i);let s=o[Li];s!==null&&s.detachView(o[ee]),i[Ft]=null,i[qn]=null,i[ae]&=-129}return i}function LR(t,n,e,i){let r=ft+i,o=e.length;i>0&&(e[r-1][qn]=n),i<o-ft?(n[qn]=e[r],Kp(e,ft+i,n)):(e.push(n),n[qn]=null),n[Ft]=e;let s=n[Zr];s!==null&&e!==s&&hx(s,n);let a=n[Li];a!==null&&a.insertView(t),vu(n),n[ae]|=128}function hx(t,n){let e=t[ns],i=n[Ft];if(fr(i))t[ae]|=2;else{let r=i[Ft][pn];n[pn]!==r&&(t[ae]|=2)}e===null?t[ns]=[n]:e.push(n)}var to=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ee];return zc(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[Dt]}set context(n){this._lView[Dt]=n}get destroyed(){return is(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Ft];if(Yn(n)){let e=n[Tc],i=e?e.indexOf(this):-1;i>-1&&($c(n,i),Ic(e,i))}this._attachedToViewContainer=!1}cm(this._lView[ee],this._lView)}onDestroy(n){bu(this._lView,n)}markForCheck(){Y_(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[ae]&=-129}reattach(){vu(this._lView),this._lView[ae]|=128}detectChanges(){this._lView[ae]|=1024,ax(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new k(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=ea(this._lView),e=this._lView[Zr];e!==null&&!n&&B_(e,this._lView),zw(this._lView[ee],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new k(902,!1);this._appRef=n;let e=ea(this._lView),i=this._lView[Zr];i!==null&&!e&&hx(i,this._lView),vu(this._lView)}};var gt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=BR;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Xc(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new to(o)}}return t})();function BR(){return fm(Tt(),K())}function fm(t,n){return t.type&4?new gt(n,t,ma(t,n)):null}function fa(t,n,e,i,r){let o=t.data[n];if(o===null)o=VR(t,n,e,i,r),X0()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=Y0();o.injectorIndex=s===null?-1:s.injectorIndex}return ta(o,!0),o}function VR(t,n,e,i,r){let o=_g(),s=vg(),a=s?o:o&&o.parent,c=t.data[n]=HR(t,a,e,n,i,r);return jR(t,c,o,s),c}function jR(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function HR(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return hg()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:xg(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function UR(t){let n=t[rg]??[],i=t[Ft][Le],r=[];for(let o of n)o.data[ww]!==void 0?r.push(o):zR(o,i);t[rg]=r}function zR(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[Cw];for(;e<r;){let o=i.nextSibling;Pw(n,i,!1),i=o,e++}}}var $R=()=>null,GR=()=>null;function Gu(t,n){return $R(t,n)}function px(t,n,e){return GR(t,n,e)}var gx=class{},At=class{},we=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>WR()}return t})();function WR(){let t=K(),n=Tt(),e=Qn(n.index,t);return(fr(e)?e:t)[Le]}var _x=(()=>{class t{static \u0275prov=J({token:t,providedIn:"root",factory:()=>null})}return t})();function vx(t){return t.debugInfo?.className||t.type.name||null}var Bu={},Wu=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Bu,i);return r!==Bu||e===Bu?r:this.parentInjector.get(n,e,i)}};function hm(t,n,e){return t[n]=e}function qR(t,n){return t[n]}function gn(t,n,e){if(e===tn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Z_(t,n,e,i){let r=gn(t,n,e);return gn(t,n+1,i)||r}function YR(t,n,e,i,r){let o=Z_(t,n,e,i);return gn(t,n+2,r)||o}function as(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&qT(r,o);let s=ji(t)?Qn(t.index,n):n;Y_(s,5);let a=n[Dt],c=NC(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=NC(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function NC(t,n,e,i){let r=te(null);try{return Be(Re.OutputStart,n,e),e(i)!==!1}catch(o){return yR(t,o),!1}finally{Be(Re.OutputEnd,n,e),te(r)}}function Q_(t,n,e,i,r,o,s,a){let c=Js(t),l=!1,u=null;if(!i&&c&&(u=QR(n,e,o,t.index)),u!==null){let m=u.__ngLastListenerFn__||u;m.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,l=!0}else{let m=Pn(t,e),g=i?i(m):m;ZT(e,g,o,a),i||(a.__ngNativeEl__=m);let _=r.listen(g,o,a);if(!ZR(o)){let C=i?I=>i(Zn(I[t.index])):t.index;bx(C,n,e,o,a,_,!1)}}return l}function ZR(t){return t.startsWith("animation")||t.startsWith("transition")}function QR(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[Ks],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function bx(t,n,e,i,r,o,s){let a=n.firstCreatePass?mg(n):null,c=ug(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function OC(t,n,e,i,r){let o=null,s=null,a=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let u=t.hostDirectiveOutputs[i];for(let m=0;m<u.length;m+=2){let g=u[m];if(g>=s&&g<=a)c=!0,qu(t,n,g,u[m+1],i,r);else if(g>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,qu(t,n,o,i,i,r)),c}function qu(t,n,e,i,r,o){let s=n[e],a=n[ee],l=a.data[e].outputs[i],m=s[l].subscribe(o);bx(t.index,a,n,r,o,m,!0)}function nn(){KR()}function KR(){let t=K(),n=ze(),e=Tt();if(n.firstCreatePass&&JR(n,e),e.controlDirectiveIndex===-1)return;br("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new Yu(t,n,e))}function rn(){XR()}function XR(){let t=K(),n=ze(),e=os();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new Yu(t,n,e))}var Yu=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return Pn(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];OC(this.tNode,this.lView,i,n,as(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];OC(this.tNode,this.lView,i,e,as(this.tNode,this.lView,n))}listenToDom(n,e){Q_(this.tNode,this.tView,this.lView,void 0,this.lView[Le],n,e,as(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];ls(a,c,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],u=this.lView[a];ls(l,u,c,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";CR(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=PC(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=PC(o.directive);s!==null&&i.push(...s)}}}return e}};function PC(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function JR(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}e1(t,n)}function e1(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(FC(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(FC(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let u=s[l];if(c===u)for(let m of n.directiveToIndex.values()){if(!Array.isArray(m))continue;let[g,_,C]=m;if(c>=_&&c<=C)return n.flags|=r,n.customControlIndex=g,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function FC(t,n){return t1(t,n)&&n1(t,n+"Change")}function t1(t,n){return n in t.inputs}function n1(t,n){return n in t.outputs}var o_=Symbol("BINDING");var us=new y("");function Zu(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=au(r,a);else if(o==2){let c=a,l=n[++s];i=au(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function re(t,n=0){let e=K();if(e===null)return ne(t,n);let i=Tt();return uw(i,e,Yt(t),n)}function el(){let t="invalid";throw new Error(t)}function yx(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(s);break}o1(t,n,e,a,o,c,l)}o!==null&&i!==null&&i1(e,i,o)}function i1(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new k(-301,!1);i.push(n[r],o)}}function r1(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function o1(t,n,e,i,r,o,s){let a=i.length,c=null;for(let g=0;g<a;g++){let _=i[g];c===null&&Hi(_)&&(c=_,r1(t,e,g)),zg(zu(e,n),t,_.type)}u1(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let g=0;g<a;g++){let _=i[g];_.providersResolver&&_.providersResolver(_)}let l=!1,u=!1,m=Zw(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let g=0;g<a;g++){let _=i[g];if(e.mergedAttrs=sa(e.mergedAttrs,_.hostAttrs),a1(t,e,n,m,_),d1(m,_,r),s!==null&&s.has(_)){let[I,j]=s.get(_);e.directiveToIndex.set(_.type,[m,I+e.directiveStart,j+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,m);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let C=_.type.prototype;!l&&(C.ngOnChanges||C.ngOnInit||C.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!u&&(C.ngOnChanges||C.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),m++}s1(t,e,o)}function s1(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))LC(0,n,r,i),LC(1,n,r,i),VC(n,i,!1);else{let o=e.get(r);BC(0,n,o,i),BC(1,n,o,i),VC(n,i,!0)}}}function LC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),Cx(n,o)}}function BC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),Cx(n,s)}}function Cx(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function VC(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||N_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let u=0;u<l.length;u+=2)if(l[u]===n){s??=[],s.push(l[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function a1(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=$r(r.type,!0)),s=new cs(o,Hi(r),re,null);t.blueprint[i]=s,e[i]=s,c1(t,n,i,Zw(t,e,r.hostVars,tn),r)}function c1(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;l1(s)!=a&&s.push(a),s.push(e,i,o)}}function l1(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function d1(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Hi(n)&&(e[""]=t)}}function u1(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function K_(t,n,e,i,r,o,s,a){let c=n[ee],l=c.consts,u=Fn(l,s),m=fa(c,t,e,i,u);return o&&yx(c,n,m,Fn(l,a),r),m.mergedAttrs=sa(m.mergedAttrs,m.attrs),m.attrs!==null&&Zu(m,m.attrs,!1),m.mergedAttrs!==null&&Zu(m,m.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,m),m}function X_(t,n){nw(t,n),og(n)&&t.queries.elementEnd(n)}function m1(t,n,e,i,r,o){let s=n.consts,a=Fn(s,r),c=fa(n,t,e,i,a);if(c.mergedAttrs=sa(c.mergedAttrs,c.attrs),o!=null){let l=Fn(s,o);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&Zu(c,c.attrs,!1),c.mergedAttrs!==null&&Zu(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var wx=typeof ShadowRoot<"u",f1=typeof Document<"u";function h1(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&dm.SignalBased)!==0};return r&&(o.transform=r),o})}function p1(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function g1(t,n,e){let i=n instanceof Ue?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Wu(e,i):e}function _1(t){let n=t.get(At,null);if(n===null)throw new k(407,!1);let e=t.get(_x,null),i=t.get(Oi,null),r=t.get(Yi,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function v1(t,n){let e=xx(t);return Nw(n,e,e==="svg"?sg:e==="math"?j0:null)}function b1(t){if(t?.toLowerCase()==="script")throw new k(905,!1)}function xx(t){return(t.selectors[0][0]||"div").toLowerCase()}var la=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=h1(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=p1(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=LA(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){Be(Re.DynamicComponentStart);let a=te(null);try{let c=this.componentDef,l=g1(c,r||this.ngModule,n),u=_1(l),m=u.tracingService;return m&&m.componentCreate?m.componentCreate(vx(c),()=>this.createComponentRef(u,l,e,i,o,s)):this.createComponentRef(u,l,e,i,o,s)}finally{te(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=y1(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),u=r?uR(l,r,a.encapsulation,e):v1(a,l);b1(u?.tagName);let m=e.get(us,null),g=C1(u,()=>e.get(Z,null)??yw());m&&m.addHost(g);let _=s?.some(jC)||o?.some(j=>typeof j!="function"&&j.bindings.some(jC)),C=U_(null,c,null,512|Yw(a),null,null,n,l,e,null,Ew(u,e,!0));m&&wx&&g instanceof ShadowRoot&&bu(C,()=>{m.removeHost(g)}),C[st]=u,Du(C);let I=null;try{let j=K_(st,C,2,"#host",()=>c.directiveRegistry,!0,0);Fw(l,u,j),aa(u,C),um(c,C,j),E_(c,j,C),X_(c,j),i!==void 0&&x1(j,this.ngContentSelectors,i),I=Qn(j.index,C),C[Dt]=I[Dt],q_(c,C,null)}catch(j){throw I!==null&&Gg(I),Gg(C),j}finally{Be(Re.DynamicComponentEnd),Eu()}return new Qu(this.componentType,C,!!_)}};function y1(t,n,e,i){let r=t?["ng-version","22.0.8"]:BA(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[o_].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let m=i[u];if(typeof m!="function")for(let g of m.bindings){a+=g[o_].requiredVars;let _=u+1;g.create&&(g.targetIdx=_,(o??=[]).push(g)),g.update&&(g.targetIdx=_,(s??=[]).push(g))}}let c=[n];if(i)for(let u of i){let m=typeof u=="function"?u:u.type,g=Yp(m);c.push(g)}return H_(0,null,w1(o,s),1,a,c,null,null,null,[r],null)}function C1(t,n){let e=t.getRootNode?.();return f1&&e instanceof Document?e.head:e&&wx&&e instanceof ShadowRoot?e:n().head}function w1(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function jC(t){let n=t[o_].kind;return n==="input"||n==="twoWay"}var Qu=class extends gx{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=pu(e[ee],st),this.location=ma(this._tNode,e),this.instance=Qn(this._tNode.index,e)[Dt],this.hostView=this.changeDetectorRef=new to(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=mm(i,r[ee],r,n,e);this.previousInputValues.set(n,e);let s=Qn(i.index,r);Y_(s,1)}get injector(){return new Jr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function x1(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Vt=(()=>{class t{static __NG_ELEMENT_ID__=D1}return t})();function D1(){let t=Tt();return Dx(t,K())}var s_=class t extends Vt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return ma(this._hostTNode,this._hostLView)}get injector(){return new Jr(this._hostTNode,this._hostLView)}get parentInjector(){let n=w_(this._hostTNode,this._hostLView);if(ow(n)){let e=Hu(n,this._hostLView),i=ju(n),r=e[ee].data[i+8];return new Jr(r,e)}else return new Jr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=HC(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-ft}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Gu(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,ca(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let u=new la(Wr(n)),m=i||this.parentInjector;if(!o&&u.ngModule==null){let se=this.parentInjector.get(Ue,null);se&&(o=se)}let g=Wr(u.componentType??{}),_=Gu(this._lContainer,g?.id??null),C=_?.firstChild??null,I=u.create(m,r,C,o,s,a);return this.insertImpl(I.hostView,c,ca(this._hostTNode,_)),I}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(U0(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[Ft],l=new t(c,c[en],c[Ft]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Jc(s,r,o,i),n.attachToViewContainerRef(),Kp(Ng(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=HC(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=$c(this._lContainer,e);i&&(Ic(Ng(this._lContainer),e),cm(i[ee],i))}detach(n){let e=this._adjustIndex(n,-1),i=$c(this._lContainer,e);return i&&Ic(Ng(this._lContainer),e)!=null?new to(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function HC(t){return t[Tc]}function Ng(t){return t[Tc]||(t[Tc]=[])}function Dx(t,n){let e,i=n[t.index];return Yn(i)?e=i:(e=ux(i,n,null,t),n[t.index]=e,z_(n,e)),S1(e,n,t,i),new s_(e,t,n)}function E1(t,n){let e=t[Le],i=e.createComment(""),r=Pn(n,t),o=e.parentNode(r);return $u(e,o,i,e.nextSibling(r),!1),i}var S1=M1,I1=()=>!1;function k1(t,n,e){return I1(t,n,e)}function M1(t,n,e,i){if(t[Qr])return;let r;e.type&8?r=Zn(i):r=E1(n,e),t[Qr]=r}var a_=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},c_=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)ev(n,e).matches!==null&&this.queries[e].setDirty()}},Ku=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=O1(n):this.predicate=n}},l_=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},d_=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,T1(e,o)),this.matchTNodeWithReadOption(n,e,Lu(e,n,o,!1,!1))}else i===gt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Lu(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===F||r===Vt||r===gt&&e.type&4)this.addMatch(e.index,-2);else{let o=Lu(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function T1(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function A1(t,n){return t.type&11?ma(t,n):t.type&4?fm(t,n):null}function R1(t,n,e,i){return e===-1?A1(n,t):e===-2?N1(t,n,i):Hc(t,t[ee],e,n)}function N1(t,n,e){if(e===F)return ma(n,t);if(e===gt)return fm(n,t);if(e===Vt)return Dx(n,t)}function Ex(t,n,e,i){let r=n[Li].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let u=o[l];a.push(R1(n,u,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function u_(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=Ex(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],u=n[-c];for(let m=ft;m<u.length;m++){let g=u[m];g[Zr]===g[Ft]&&u_(g[ee],g,l,i)}if(u[ns]!==null){let m=u[ns];for(let g=0;g<m.length;g++){let _=m[g];u_(_[ee],_,l,i)}}}}}return i}function J_(t,n){return t[Li].queries[n].queryList}function Sx(t,n,e){let i=new En((e&4)===4);return G0(t,n,i,i.destroy),(n[Li]??=new c_).queries.push(new a_(i))-1}function Ix(t,n,e){let i=ze();return i.firstCreatePass&&(Mx(i,new Ku(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Sx(i,K(),n)}function kx(t,n,e,i){let r=ze();if(r.firstCreatePass){let o=Tt();Mx(r,new Ku(n,e,i),o.index),P1(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Sx(r,K(),e)}function O1(t){return t.split(",").map(n=>n.trim())}function Mx(t,n,e){t.queries===null&&(t.queries=new l_),t.queries.track(new d_(n,e))}function P1(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function ev(t,n){return t.queries.getByIndex(n)}function Tx(t,n){let e=t[ee],i=ev(e,n);return i.crossesNgTemplate?u_(e,t,n,[]):Ex(e,t,i,n)}function Ax(t,n,e){let i,r=cc(()=>{i._dirtyCounter();let o=F1(i,t);if(n&&o===void 0)throw new k(-951,!1);return o});return i=r[wt],i._dirtyCounter=V(0),i._flatValue=void 0,r}function tv(t){return Ax(!0,!1,t)}function nv(t){return Ax(!0,!0,t)}function Rx(t,n){let e=t[wt];e._lView=K(),e._queryIndex=n,e._queryList=J_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function F1(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[ae]&4)return n?void 0:Xt;let r=J_(e,i),o=Tx(e,i);return r.reset(o,hw),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function yr(t){return!!t&&typeof t.then=="function"}function iv(t){return!!t&&typeof t.subscribe=="function"}var qi=class{},pm=class{};var Xu=class extends qi{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=D0(n);this._bootstrapComponents=SA(o.bootstrap),this._r3Injector=Dg(n,e,[{provide:qi,useValue:this},...i],Ec(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Ju=class extends pm{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Xu(this.moduleType,n,[])}};var Gc=class extends qi{injector;instance=null;constructor(n){super();let e=new Zo([...n.providers,{provide:qi,useValue:this}],n.parent||Qs(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function tl(t,n,e=null){return new Gc({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var L1=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Jp(!1,e.type),r=i.length>0?tl([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=J({token:t,providedIn:"environment",factory:()=>new t(ne(Ue))})}return t})();function D(t){return qc(()=>{let n=Nx(t),e=Q(b({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==x_.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(L1).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||yi.Emulated,styles:t.styles||Xt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&br("NgStandalone"),Ox(e);let i=t.dependencies;return e.directiveDefs=UC(i,B1),e.pipeDefs=UC(i,E0),e.id=H1(e),e})}function B1(t){return Wr(t)||Yp(t)}function de(t){return qc(()=>({type:t.type,bootstrap:t.bootstrap||Xt,declarations:t.declarations||Xt,imports:t.imports||Xt,exports:t.exports||Xt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function V1(t,n){if(t==null)return qr;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=dm.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function j1(t){if(t==null)return qr;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function T(t){return qc(()=>{let n=Nx(t);return Ox(n),n})}function no(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Nx(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||qr,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Xt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:V1(t.inputs,n),outputs:j1(t.outputs),debugInfo:null}}function Ox(t){t.features?.forEach(n=>n(t))}function UC(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function H1(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var rv=new y("");function gm(t){return pi([{provide:rv,multi:!0,useValue:t}])}var ov=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(rv,{optional:!0})??[];injector=d(q);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=zt(this.injector,r);if(yr(o))e.push(o);else if(iv(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function _m(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function U1(t){return Object.getPrototypeOf(t.prototype).constructor}function be(t){let n=U1(t.type),e=!0,i=[t];for(;n;){let r;if(Hi(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new k(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=Og(t.inputs),s.declaredInputs=Og(t.declaredInputs),s.outputs=Og(t.outputs);let a=r.hostBindings;a&&q1(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&G1(t,c),l&&W1(t,l),z1(t,r),x0(t.outputs,r.outputs),Hi(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===be&&(e=!1)}}n=Object.getPrototypeOf(n)}$1(i)}function z1(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function $1(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=sa(r.hostAttrs,e=sa(e,r.hostAttrs))}}function Og(t){return t===qr?{}:t===Xt?[]:t}function G1(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function W1(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function q1(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function Px(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=sa(t.mergedAttrs,t.attrs);let u=t.tView=H_(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),ta(t,!1);let c=Z1(e,n,t,i);Su()&&V_(e,n,c,t),aa(c,n);let l=ux(c,n,c,t);n[i+st]=l,z_(n,l),k1(l,t,n)}function Y1(t,n,e,i,r,o,s,a,c,l,u){let m=e+st,g;return n.firstCreatePass?(g=fa(n,m,4,s||null,a||null),yu()&&yx(n,t,g,Fn(n.consts,l),$_),nw(n,g)):g=n.data[m],Px(g,t,n,e,i,r,o,c),Js(g)&&um(n,t,g),l!=null&&Kc(t,g,u),g}function da(t,n,e,i,r,o,s,a,c,l,u){let m=e+st,g;if(n.firstCreatePass){if(g=fa(n,m,4,s||null,a||null),l!=null){let _=Fn(n.consts,l);g.localNames=[];for(let C=0;C<_.length;C+=2)g.localNames.push(_[C],-1)}}else g=n.data[m];return Px(g,t,n,e,i,r,o,c),l!=null&&Kc(t,g,u),g}function Ge(t,n,e,i,r,o,s,a){let c=K(),l=ze(),u=Fn(l.consts,o);return Y1(c,l,t,n,e,i,r,u,void 0,s,a),Ge}function ha(t,n,e,i,r,o,s,a){let c=K(),l=ze(),u=Fn(l.consts,o);return da(c,l,t,n,e,i,r,u,void 0,s,a),ha}var Z1=Q1;function Q1(t,n,e,i){return Nc(!0),n[Le].createComment("")}var vm=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var sv=new y("");var nl=new y("");function Fx(){up(()=>{let t="";throw new k(600,t)})}var K1=10;var Sn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Ln);afterRenderManager=d(sm);zonelessEnabled=d(Pc);rootEffectScheduler=d(Mu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new S;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(hr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Y(e=>!e))}constructor(){d(Yi,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Ue);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=q.NULL){return this._injector.get(B).run(()=>{if(Be(Re.BootstrapComponentStart),!this._injector.get(ov).done){let se="";throw new k(405,se)}let a=Wr(e),c=this._injector.get(qi),l=new la(a,c);this.componentTypes.push(e);let{hostElement:u,directives:m,bindings:g}=X1(i),_=u||l.selector,C=l.create(r,[],_,c.injector,m,g),I=C.location.nativeElement,j=C.injector.get(sv,null);return j?.registerApplication(I),C.onDestroy(()=>{this.detachView(C.hostView),jc(this.components,C),j?.unregisterApplication(I)}),this._loadComponent(C),Be(Re.BootstrapComponentEnd,C),C})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Be(Re.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(om.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Be(Re.ChangeDetectionEnd),new k(101,!1);let e=te(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,te(e),this.afterTick.next(),Be(Re.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(At,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<K1;){Be(Re.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Be(Re.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ac(r))continue;let o=i&&!this.zonelessEnabled?0:1;ax(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Ac(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;jc(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(nl,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>jc(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new k(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function X1(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function jc(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function bm(t,n){let e=K(),i=Ui();if(gn(e,i,n)){let r=ze(),o=os();if(mm(o,r,e,t,n))ji(o)&&ex(e,o.index);else{let a=Pn(o,e);tx(e[Le],a,null,o.value,t,n,null)}}return bm}function G(t,n,e,i){let r=K(),o=Ui();if(gn(r,o,n)){let s=ze(),a=os();vR(a,r,t,n,e,i)}return G}var m_=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Pg(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function J1(t,n,e,i){let r,o,s=0,a=t.length-1,c=void 0;if(Array.isArray(n)){te(i);let l=n.length-1;for(te(null);s<=a&&s<=l;){let u=t.at(s),m=n[s],g=Pg(s,u,s,m,e);if(g!==0){g<0&&t.updateValue(s,m),s++;continue}let _=t.at(a),C=n[l],I=Pg(a,_,l,C,e);if(I!==0){I<0&&t.updateValue(a,C),a--,l--;continue}let j=e(s,u),se=e(a,_),Ne=e(s,m);if(Object.is(Ne,se)){let ut=e(l,C);Object.is(ut,j)?(t.swap(s,a),t.updateValue(a,C),l--,a--):t.move(a,s),t.updateValue(s,m),s++;continue}if(r??=new em,o??=$C(t,s,a,e),f_(t,r,s,Ne))t.updateValue(s,m),s++,a++;else if(o.has(Ne))r.set(j,t.detach(s)),a--;else{let ut=t.create(s,n[s]);t.attach(s,ut),s++,a++}}for(;s<=l;)zC(t,r,e,s,n[s]),s++}else if(n!=null){te(i);let l=n[Symbol.iterator]();te(null);let u=l.next();for(;!u.done&&s<=a;){let m=t.at(s),g=u.value,_=Pg(s,m,s,g,e);if(_!==0)_<0&&t.updateValue(s,g),s++,u=l.next();else{r??=new em,o??=$C(t,s,a,e);let C=e(s,g);if(f_(t,r,s,C))t.updateValue(s,g),s++,a++,u=l.next();else if(!o.has(C))t.attach(s,t.create(s,g)),s++,a++,u=l.next();else{let I=e(s,m);r.set(I,t.detach(s)),a--}}}for(;!u.done;)zC(t,r,e,t.length,u.value),u=l.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(l=>{t.destroy(l)})}function f_(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function zC(t,n,e,i,r){if(f_(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function $C(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var em=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function O(t,n,e,i,r,o,s,a){br("NgControlFlow");let c=K(),l=ze(),u=Fn(l.consts,o);return da(c,l,t,n,e,i,r,u,256,s,a),av}function av(t,n,e,i,r,o,s,a){br("NgControlFlow");let c=K(),l=ze(),u=Fn(l.consts,o);return da(c,l,t,n,e,i,r,u,512,s,a),av}function P(t,n){br("NgControlFlow");let e=K(),i=Ui(),r=e[i]!==tn?e[i]:-1,o=r!==-1?tm(e,st+r):void 0,s=0;if(gn(e,i,t)){let a=te(null);try{if(o!==void 0&&fx(o,s),t!==-1){let c=st+t,l=tm(e,c),u=__(e[ee],c),m=px(l,u,e),g=Xc(e,u,n,{dehydratedView:m});Jc(l,g,s,ca(u,m))}}finally{te(a)}}else if(o!==void 0){let a=mx(o,s);a!==void 0&&(a[Dt]=n)}}var h_=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-ft}};function ym(t){return t}function Cr(t,n){return n}var p_=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function at(t,n,e,i,r,o,s,a,c,l,u,m,g){br("NgControlFlow");let _=K(),C=ze(),I=c!==void 0,j=K(),se=a?s.bind(j[pn][Dt]):s,Ne=new p_(I,se);j[st+t]=Ne,da(_,C,t+1,n,e,i,r,Fn(C.consts,o),256),I&&da(_,C,t+2,c,l,u,m,Fn(C.consts,g),512)}var g_=class extends m_{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-ft}at(n){return this.getLView(n)[Dt].$implicit}attach(n,e){let i=e[Jo];this.needsIndexUpdate||=n!==this.length,Jc(this.lContainer,e,n,ca(this.templateTNode,i)),eN(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,tN(this.lContainer,n),nN(this.lContainer,n)}create(n,e){let i=Gu(this.lContainer,this.templateTNode.tView.ssrId);return Xc(this.hostLView,this.templateTNode,new h_(this.lContainer,e,n),{dehydratedView:i})}destroy(n){cm(n[ee],n)}updateValue(n,e){this.getLView(n)[Dt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[Dt].$index=n}getLView(n){return iN(this.lContainer,n)}};function ct(t){let n=te(null),e=zi();try{let i=K(),r=i[ee],o=i[e],s=e+1,a=tm(i,s);if(o.liveCollection===void 0){let l=__(r,s);o.liveCollection=new g_(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(J1(c,t,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=Ui(),u=c.length===0;if(gn(i,l,u)){let m=e+2,g=tm(i,m);if(u){let _=__(r,m),C=px(g,_,i),I=Xc(i,_,void 0,{dehydratedView:C});Jc(g,I,0,ca(_,C))}else r.firstUpdatePass&&UR(g),fx(g,0)}}}finally{te(n)}}function tm(t,n){return t[n]}function eN(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e],r=i?i[Vi]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Fi];GA(o,r),eo.delete(i[Bi]),r.detachedLeaveAnimationFns=void 0}}function tN(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e],r=i?i[Vi]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function nN(t,n){return $c(t,n)}function iN(t,n){return mx(t,n)}function __(t,n){return pu(t,n)}function x(t,n,e){let i=K(),r=Ui();if(gn(i,r,n)){let o=ze(),s=os();Xw(s,i,t,n,i[Le],e)}return x}function v_(t,n,e,i,r){mm(n,t,e,r?"class":"style",i)}function f(t,n,e,i){let r=K(),o=r[ee],s=t+st,a=o.firstCreatePass?K_(s,r,2,n,$_,yu(),e,i):o.data[s];if(ji(a)){let c=r[_i].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(vx(l),()=>(GC(t,n,r,a,i),f))}}return GC(t,n,r,a,i),f}function GC(t,n,e,i,r){if(G_(i,e,t,n,Lx),Js(i)){let o=e[ee];um(o,e,i),E_(o,i,e)}r!=null&&Kc(e,i)}function h(){let t=ze(),n=Tt(),e=W_(n);return t.firstCreatePass&&X_(t,e),pg(e)&&gg(),fg(),e.classesWithoutHost!=null&&MT(e)&&v_(t,e,K(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&TT(e)&&v_(t,e,K(),e.stylesWithoutHost,!1),h}function M(t,n,e,i){return f(t,n,e,i),h(),M}function Ve(t,n,e,i){let r=K(),o=r[ee],s=t+st,a=o.firstCreatePass?m1(s,o,2,n,e,i):o.data[s];return G_(a,r,t,n,Lx),i!=null&&Kc(r,a),Ve}function Qe(){let t=Tt(),n=W_(t);return pg(n)&&gg(),fg(),Qe}function In(t,n,e,i){return Ve(t,n,e,i),Qe(),In}var Lx=(t,n,e,i,r)=>(Nc(!0),Nw(n[Le],i,xg()));function cv(t,n,e){let i=K(),r=i[ee],o=t+st,s=r.firstCreatePass?K_(o,i,8,"ng-container",$_,yu(),n,e):r.data[o];if(G_(s,i,t,"ng-container",rN),Js(s)){let a=i[ee];um(a,i,s),E_(a,s,i)}return e!=null&&Kc(i,s),cv}function lv(){let t=ze(),n=Tt(),e=W_(n);return t.firstCreatePass&&X_(t,e),lv}function on(t,n,e){return cv(t,n,e),lv(),on}var rN=(t,n,e,i,r)=>(Nc(!0),CA(n[Le],""));function lt(){return K()}function ht(t,n,e){let i=K(),r=Ui();if(gn(i,r,n)){let o=ze(),s=os();Jw(s,i,t,n,i[Le],e)}return ht}var Lc=void 0;function oN(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var sN=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Lc,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Lc,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Lc,Lc,Lc],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",oN],Fg=Object.create(null);function _n(t){let n=aN(t),e=WC(n);if(e)return e;let i=n.split("-")[0];if(e=WC(i),e)return e;if(i==="en")return sN;throw new k(701,!1)}function WC(t){if(!(t in Fg)){let n=Wn.ng&&Wn.ng.common&&Wn.ng.common.locales&&Wn.ng.common.locales[t];return n!==void 0&&(Fg[t]=n),n}return Fg[t]}var bt={LocaleId:0,DayPeriodsFormat:1,DayPeriodsStandalone:2,DaysFormat:3,DaysStandalone:4,MonthsFormat:5,MonthsStandalone:6,Eras:7,FirstDayOfWeek:8,WeekendRange:9,DateFormat:10,TimeFormat:11,DateTimeFormat:12,NumberSymbols:13,NumberFormats:14,CurrencyCode:15,CurrencySymbol:16,CurrencyName:17,Currencies:18,Directionality:19,PluralCase:20,ExtraData:21};function aN(t){return t.toLowerCase().replace(/_/g,"-")}var il="en-US",cN="USD";var lN=il;function Bx(t){typeof t=="string"&&(lN=t.toLowerCase().replace(/_/g,"-"))}function A(t,n,e){let i=K(),r=ze(),o=Tt();return Vx(r,i,i[Le],o,t,n,e),A}function pa(t,n,e){let i=K(),r=ze(),o=Tt();return(o.type&3||e)&&Q_(o,r,i,e,i[Le],t,n,as(o,i,n)),pa}function Vx(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=as(i,n,o),Q_(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let m=0;m<u.length;m+=2){let g=u[m],_=u[m+1];c??=as(i,n,o),qu(i,n,g,_,r,c)}if(l&&l.length)for(let m of l)c??=as(i,n,o),qu(i,n,m,r,r,c)}}function E(t=1){return oC(t)}function dN(t,n){let e=null,i=RA(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?jw(t,o,!0):PA(i,o))return r}return e}function ke(t){let n=K()[pn][en];if(!n.projection){let e=t?t.length:1,i=n.projection=A0(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?dN(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function ie(t,n=0,e,i,r,o){let s=K(),a=ze(),c=i?t+1:null;c!==null&&da(s,a,c,i,r,o,null,e);let l=fa(a,st+t,16,null,e||null);l.projection===null&&(l.projection=n),bg();let m=!s[Jo]||hg();s[pn][en].projection[l.projection]===null&&c!==null?uN(s,a,c):m&&!im(l)&&oR(a,s,l)}function uN(t,n,e){let i=st+e,r=n.data[i],o=t[i],s=Gu(o,r.tView.ssrId),a=Xc(t,r,void 0,{dehydratedView:s});Jc(o,a,0,ca(r,s))}function yt(t,n,e,i){return kx(t,n,e,i),yt}function je(t,n,e){return Ix(t,n,e),je}function z(t){let n=K(),e=ze(),i=xu();Rc(i+1);let r=ev(e,i);if(t.dirty&&H0(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Tx(n,i);t.reset(o,hw),t.notifyOnChanges()}return!0}return!1}function $(){return J_(K(),xu())}function Cm(t,n,e,i,r){return Rx(n,kx(t,e,i,r)),Cm}function wm(t,n,e,i){return Rx(t,Ix(n,e,i)),wm}function xm(t=1){Rc(xu()+t)}function He(t){let n=Z0();return gu(n,st+t)}function Ou(t,n){return t<<17|n<<2}function ds(t){return t>>17&32767}function mN(t){return(t&2)==2}function fN(t,n){return t&131071|n<<17}function b_(t){return t|2}function ua(t){return(t&131068)>>2}function Lg(t,n){return t&-131069|n<<2}function hN(t){return(t&1)===1}function y_(t){return t|1}function pN(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=ds(s),c=ua(s);t[i]=e;let l=!1,u;if(Array.isArray(e)){let m=e;u=m[1],(u===null||Zs(m,u)>0)&&(l=!0)}else u=e;if(r)if(c!==0){let g=ds(t[a+1]);t[i+1]=Ou(g,a),g!==0&&(t[g+1]=Lg(t[g+1],i)),t[a+1]=fN(t[a+1],i)}else t[i+1]=Ou(a,0),a!==0&&(t[a+1]=Lg(t[a+1],i)),a=i;else t[i+1]=Ou(c,0),a===0?a=i:t[c+1]=Lg(t[c+1],i),c=i;l&&(t[i+1]=b_(t[i+1])),qC(t,u,i,!0),qC(t,u,i,!1),gN(n,u,t,i,o),s=Ou(a,c),o?n.classBindings=s:n.styleBindings=s}function gN(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Zs(o,n)>=0&&(e[i+1]=y_(e[i+1]))}function qC(t,n,e,i){let r=t[e+1],o=n===null,s=i?ds(r):ua(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];_N(c,n)&&(a=!0,t[s+1]=i?y_(l):b_(l)),s=i?ds(l):ua(l)}a&&(t[e+1]=i?b_(r):y_(r))}function _N(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Zs(t,n)>=0:!1}var bi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function vN(t){return t.substring(bi.key,bi.keyEnd)}function bN(t){return yN(t),jx(t,Hx(t,0,bi.textEnd))}function jx(t,n){let e=bi.textEnd;return e===n?-1:(n=bi.keyEnd=CN(t,bi.key=n,e),Hx(t,n,e))}function yN(t){bi.key=0,bi.keyEnd=0,bi.value=0,bi.valueEnd=0,bi.textEnd=t.length}function Hx(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function CN(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function wr(t,n,e){return Ux(t,n,e,!1),wr}function H(t,n){return Ux(t,n,null,!0),H}function jt(t){xN(MN,wN,t,!0)}function wN(t,n){for(let e=bN(n);e>=0;e=jx(n,e))mu(t,vN(n),!0)}function Ux(t,n,e,i){let r=K(),o=ze(),s=Cu(2);if(o.firstUpdatePass&&$x(o,t,s,i),n!==tn&&gn(r,s,n)){let a=o.data[zi()];Gx(o,a,r,r[Le],t,r[s+1]=AN(n,e),i,s)}}function xN(t,n,e,i){let r=ze(),o=Cu(2);r.firstUpdatePass&&$x(r,null,o,i);let s=K();if(e!==tn&&gn(s,o,e)){let a=r.data[zi()];if(Wx(a,i)&&!zx(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=au(c,e||"")),v_(r,a,s,e,i)}else TN(r,a,s,s[Le],s[o+1],s[o+1]=kN(t,n,e),i,o)}}function zx(t,n){return n>=t.expandoStartIndex}function $x(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[zi()],s=zx(t,e);Wx(o,i)&&n===null&&!s&&(n=!1),n=DN(r,o,n,i),pN(r,o,n,e,s,i)}}function DN(t,n,e,i){let r=tC(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Bg(null,t,n,e,i),e=Wc(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=Bg(r,t,n,e,i),o===null){let c=EN(t,n,i);c!==void 0&&Array.isArray(c)&&(c=Bg(null,t,n,c[1],i),c=Wc(c,n.attrs,i),SN(t,n,i,c))}else o=IN(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function EN(t,n,e){let i=e?n.classBindings:n.styleBindings;if(ua(i)!==0)return t[ds(i)]}function SN(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[ds(r)]=i}function IN(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Wc(i,s,e)}return Wc(i,n.attrs,e)}function Bg(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Wc(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Wc(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),mu(t,s,e?!0:n[++o]))}return t===void 0?null:t}function kN(t,n,e){if(e==null||e==="")return Xt;let i=[],r=Ci(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function MN(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&mu(t,i,e)}function TN(t,n,e,i,r,o,s,a){r===tn&&(r=Xt);let c=0,l=0,u=0<r.length?r[0]:null,m=0<o.length?o[0]:null;for(;u!==null||m!==null;){let g=c<r.length?r[c+1]:void 0,_=l<o.length?o[l+1]:void 0,C=null,I;u===m?(c+=2,l+=2,g!==_&&(C=m,I=_)):m===null||u!==null&&u<m?(c+=2,C=u):(l+=2,C=m,I=_),C!==null&&Gx(t,n,e,i,C,I,s,a),u=c<r.length?r[c]:null,m=l<o.length?o[l]:null}}function Gx(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],u=hN(l)?YC(c,n,e,r,ua(l),s):void 0;if(!nm(u)){nm(o)||mN(l)&&(o=YC(c,null,e,r,a,s));let m=ag(zi(),e);aR(i,s,m,r,o)}}function YC(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),u=l?c[1]:c,m=u===null,g=e[r+1];g===tn&&(g=m?Xt:void 0);let _=m?fu(g,i):u===i?g:void 0;if(l&&!nm(_)&&(_=fu(c,i)),nm(_)&&(a=_,s))return a;let C=t[r+1];r=s?ds(C):ua(C)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=fu(c,i))}return a}function nm(t){return t!==void 0}function AN(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Ec(Ci(t)))),t}function Wx(t,n){return(t.flags&(n?8:16))!==0}function v(t,n=""){let e=K(),i=ze(),r=t+st,o=i.firstCreatePass?fa(i,r,1,n,null):i.data[r],s=RN(i,e,o,n);e[r]=s,Su()&&V_(i,e,s,o),ta(o,!1)}var RN=(t,n,e,i)=>(Nc(!0),bA(n[Le],i));function qx(t,n,e,i=""){return gn(t,Ui(),e)?n+Ko(e)+i:tn}function NN(t,n,e,i,r,o=""){let s=Q0(),a=Z_(t,s,e,r);return Cu(2),a?n+Ko(e)+i+Ko(r)+o:tn}function X(t){return oe("",t),X}function oe(t,n,e){let i=K(),r=qx(i,t,n,e);return r!==tn&&Yx(i,zi(),r),oe}function Dm(t,n,e,i,r){let o=K(),s=NN(o,t,n,e,i,r);return s!==tn&&Yx(o,zi(),s),Dm}function Yx(t,n,e){let i=ag(n,t);yA(t[Le],i,e)}function xr(t,n,e){Tu(n)&&(n=n());let i=K(),r=Ui();if(gn(i,r,n)){let o=ze(),s=os();Xw(s,i,t,n,i[Le],e)}return xr}function io(t,n){let e=Tu(t);return e&&t.set(n),e}function Dr(t,n){let e=K(),i=ze(),r=Tt();return Vx(i,e,e[Le],r,t,n),Dr}function kn(t){return gn(K(),Ui(),t)?Ko(t):tn}function Jn(t,n,e=""){return qx(K(),t,n,e)}function ZC(t,n,e){let i=ze();i.firstCreatePass&&Zx(n,i.data,i.blueprint,Hi(t),e)}function Zx(t,n,e,i,r){if(t=Yt(t),Array.isArray(t))for(let o=0;o<t.length;o++)Zx(t[o],n,e,i,r);else{let o=ze(),s=K(),a=Tt(),c=Yo(t)?t:Yt(t.provide),l=tg(t),u=a.providerIndexes&1048575,m=a.directiveStart,g=a.providerIndexes>>20;if(Yo(t)||!t.multi){let _=new cs(l,r,re,null),C=jg(c,n,r?u:u+g,m);C===-1?(zg(zu(a,s),o,c),Vg(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(_),s.push(_)):(e[C]=_,s[C]=_)}else{let _=jg(c,n,u+g,m),C=jg(c,n,u,u+g),I=_>=0&&e[_],j=C>=0&&e[C];if(r&&!j||!r&&!I){zg(zu(a,s),o,c);let se=FN(r?PN:ON,e.length,r,i,l,t);!r&&j&&(e[C].providerFactory=se),Vg(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(se),s.push(se)}else{let se=Qx(e[r?C:_],l,!r&&i);Vg(o,t,_>-1?_:C,se)}!r&&i&&j&&e[C].componentProviders++}}}function Vg(t,n,e,i){let r=Yo(n),o=L0(n);if(r||o){let c=(o?Yt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=l.indexOf(e);u===-1?l.push(e,[i,c]):l[u+1].push(i,c)}else l.push(e,c)}}}function Qx(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function jg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function ON(t,n,e,i,r){return C_(this.multi,[])}function PN(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Hc(i,i[ee],this.providerFactory.index,r);s=c.slice(0,a),C_(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],C_(o,s);return s}function C_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function FN(t,n,e,i,r,o){let s=new cs(t,e,re,null);return s.multi=[],s.index=n,s.componentProviders=0,Qx(s,r,i&&!e),s}function Oe(t,n){return e=>{e.providersResolver=(i,r)=>ZC(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>ZC(i,r?r(n):n,!0))}}function dv(t,n){let e=na()+t,i=K();return i[e]===tn?hm(i,e,n()):qR(i,e)}function ga(t,n,e){return Kx(K(),na(),t,n,e)}function uv(t,n,e,i,r){return BN(K(),na(),t,n,e,i,r)}function mv(t,n){let e=t[n];return e===tn?void 0:e}function Kx(t,n,e,i,r,o){let s=n+e;return gn(t,s,r)?hm(t,s+1,o?i.call(o,r):i(r)):mv(t,s+1)}function LN(t,n,e,i,r,o,s){let a=n+e;return Z_(t,a,r,o)?hm(t,a+2,s?i.call(s,r,o):i(r,o)):mv(t,a+2)}function BN(t,n,e,i,r,o,s,a){let c=n+e;return YR(t,c,r,o,s)?hm(t,c+3,a?i.call(a,r,o,s):i(r,o,s)):mv(t,c+3)}function xe(t,n){let e=ze(),i,r=t+st;e.firstCreatePass?(i=VN(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=$r(i.type,!0)),s,a=hn(re);try{let c=Uu(!1),l=o();return Uu(c),cg(e,K(),r,l),l}finally{hn(a)}}function VN(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Me(t,n,e){let i=t+st,r=K(),o=gu(r,i);return Xx(r,i)?Kx(r,na(),n,o.transform,e,o):o.transform(e)}function ms(t,n,e,i){let r=t+st,o=K(),s=gu(o,r);return Xx(o,r)?LN(o,na(),n,s.transform,e,i,s):s.transform(e,i)}function Xx(t,n){return t[ee].data[n].pure}function ei(t,n){return fm(t,n)}var Jx=(()=>{class t{applicationErrorHandler=d(Ln);appRef=d(Sn);taskService=d(hr);ngZone=d(B);zonelessEnabled=d(Pc);tracing=d(Yi,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new he;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(xc):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Mg,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?lC:Eg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(xc+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function eD(){return[{provide:Oi,useExisting:Jx},{provide:B,useClass:Dc},{provide:Pc,useValue:!0}]}var fv=(()=>{class t{compileModuleSync(e){return new Ju(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function jN(){return typeof $localize<"u"&&$localize.locale||il}var _a=new y("",{factory:()=>d(_a,{optional:!0,skipSelf:!0})||jN()}),hv=new y("",{factory:()=>cN});var Em=class{destroyed=!1;listeners=null;errorHandler=d(Jt,{optional:!0});isEmitting=!1;hasNullListeners=!1;destroyRef=d(Lt);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new k(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let e=this.listeners?this.listeners.indexOf(n):-1;e>-1&&(this.isEmitting?(this.hasNullListeners=!0,this.listeners[e]=null):this.listeners.splice(e,1))}}}emit(n){if(this.destroyed){console.warn(Pi(953,!1));return}if(this.listeners===null)return;this.isEmitting=!0;let e=te(null);try{for(let i of this.listeners)try{i!==null&&i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{this.hasNullListeners&&(this.hasNullListeners=!1,this.listeners&&HN(this.listeners)),te(e),this.isEmitting=!1}}};function HN(t){let n=t.length-1;for(;n>-1;)t[n]===null&&t.splice(n,1),n--}function dt(t,n){return cc(t,n?.equal)}function Ce(t){return $y(t)}var UN=t=>t;function Sm(t,n){if(typeof t=="function"){let e=gp(t,UN,n?.equal);return tD(e,n?.debugName)}else{let e=gp(t.source,t.computation,t.equal);return tD(e,t.debugName)}}function tD(t,n){let e=t[wt],i=t;return i.set=r=>Uy(e,r),i.update=r=>zy(e,r),i.asReadonly=Iu.bind(t),i}var aD=Symbol("InputSignalNode#UNSET"),nO=Q(b({},lc),{transformFn:void 0,applyValueToInputSignal(t,n){Fo(t,n)}});function cD(t,n){let e=Object.create(nO);e.value=t,e.transformFn=n?.transform;function i(){if(Br(e),e.value===aD){let r=null;throw new k(-950,r)}return e.value}return i[wt]=e,i}var ti=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Yc(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function vv(t){return iO(t)?t.default:t}function iO(t){return t&&typeof t=="object"&&"default"in t}function km(t){return new Em}function nD(t,n){return cD(t,n)}function rO(t){return cD(aD,t)}var vn=(nD.required=rO,nD);function iD(t,n){return tv(n)}function oO(t,n){return nv(n)}var ol=(iD.required=oO,iD);function rD(t,n){return tv(n)}function sO(t,n){return nv(n)}var lD=(rD.required=sO,rD);var aO=1e4;var OZ=aO-1e3;var ge=(()=>{class t{static __NG_ELEMENT_ID__=cO}return t})();function cO(t){return lO(Tt(),K(),(t&16)===16)}function lO(t,n,e){if(ji(t)&&!e){let i=Qn(t.index,n);return new to(i,i)}else if(t.type&175){let i=n[pn];return new to(i,n)}return null}var gv=new y(""),dO=new y("");function rl(t){return!t.moduleRef}function uO(t){let n=rl(t)?t.r3Injector:t.moduleRef.injector,e=n.get(B);return e.run(()=>{rl(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Ln),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),rl(t)){let o=()=>n.destroy(),s=t.platformInjector.get(gv);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(gv);s.add(o),t.moduleRef.onDestroy(()=>{jc(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return fO(i,e,()=>{let o=n.get(hr),s=o.add(),a=n.get(ov);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(_a,il);if(Bx(c||il),!n.get(dO,!0))return rl(t)?n.get(Sn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(rl(t)){let u=n.get(Sn);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return mO?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var mO;function fO(t,n,e){try{let i=e();return yr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Im=null;function hO(t=[],n){return q.create({name:n,providers:[{provide:Mc,useValue:"platform"},{provide:gv,useValue:new Set([()=>Im=null])},...t]})}function pO(t=[]){if(Im)return Im;let n=hO(t);return Im=n,Fx(),gO(n),n}function gO(t){let n=t.get(ku,null);zt(t,()=>{n?.forEach(e=>e())})}function dD(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Be(Re.BootstrapApplicationStart);try{let o=r?.injector??pO(i),s=[eD(),uC,...e||[]],a=new Gc({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return uO({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Be(Re.BootstrapApplicationEnd)}}function L(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Et(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var pv=Symbol("NOT_SET"),uD=new Set,_O=Q(b({},lc),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:pv,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==pv&&!Vs(this))return this.signal;try{for(let r of this.cleanup??uD)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=lr(this),i;try{i=this.userFn.apply(null,n)}finally{Vr(this,e)}return(this.value===pv||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),_v=class extends Uc{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Lt),s),this.scheduler=r;for(let a of P_){let c=e[a];if(c===void 0)continue;let l=Object.create(_O);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Br(l),l.value),l.signal[wt]=l,l.registerCleanupFn=u=>(l.cleanup??=new Set).add(u),this.nodes[a]=l,this.hooks[a]=u=>l.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??uD)e()}finally{jr(n)}}};function bv(t,n){let e=n?.injector??d(q),i=e.get(Oi),r=e.get(sm),o=e.get(Yi,null,{optional:!0});r.impl??=e.get(F_);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(ia,null,{optional:!0}),c=new _v(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Mm(t,n){let e=Wr(t),i=n.elementInjector||Qs();return new la(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var mD=null;function ni(){return mD}function yv(t){mD??=t}var sl=class{},va=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:()=>d(fD),providedIn:"platform"})}return t})();var fD=(()=>{class t extends va{_location;_history;_doc=d(Z);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return ni().getBaseHref(this._doc)}onPopState(e){let i=ni().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=ni().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function gD(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function hD(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function ro(t){return t&&t[0]!=="?"?`?${t}`:t}var ba=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:()=>d(bO),providedIn:"root"})}return t})(),vO=new y(""),bO=(()=>{class t extends ba{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(Z).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return gD(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+ro(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+ro(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+ro(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(ne(va),ne(vO,8))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var oo=(()=>{class t{_subject=new S;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=wO(hD(pD(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+ro(i))}normalize(e){return t.stripTrailingSlash(CO(this._basePath,pD(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ro(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ro(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=ro;static joinWithSlash=gD;static stripTrailingSlash=hD;static \u0275fac=function(i){return new(i||t)(ne(ba))};static \u0275prov=J({token:t,factory:()=>yO(),providedIn:"root"})}return t})();function yO(){return new oo(ne(ba))}function CO(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function pD(t){return t.replace(/\/index\.html$/,"")}function wO(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var yD={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},Sv=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(Sv||{});var sn=(function(t){return t[t.Format=0]="Format",t[t.Standalone=1]="Standalone",t})(sn||{}),We=(function(t){return t[t.Narrow=0]="Narrow",t[t.Abbreviated=1]="Abbreviated",t[t.Wide=2]="Wide",t[t.Short=3]="Short",t})(We||{}),Mn=(function(t){return t[t.Short=0]="Short",t[t.Medium=1]="Medium",t[t.Long=2]="Long",t[t.Full=3]="Full",t})(Mn||{}),Tn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function CD(t){return _n(t)[bt.LocaleId]}function wD(t,n,e){let i=_n(t),r=[i[bt.DayPeriodsFormat],i[bt.DayPeriodsStandalone]],o=ii(r,n);return ii(o,e)}function xD(t,n,e){let i=_n(t),r=[i[bt.DaysFormat],i[bt.DaysStandalone]],o=ii(r,n);return ii(o,e)}function DD(t,n,e){let i=_n(t),r=[i[bt.MonthsFormat],i[bt.MonthsStandalone]],o=ii(r,n);return ii(o,e)}function ED(t,n){let i=_n(t)[bt.Eras];return ii(i,n)}function al(t,n){let e=_n(t);return ii(e[bt.DateFormat],n)}function cl(t,n){let e=_n(t);return ii(e[bt.TimeFormat],n)}function ll(t,n){let i=_n(t)[bt.DateTimeFormat];return ii(i,n)}function Zi(t,n){let e=_n(t),i=e[bt.NumberSymbols][n];if(typeof i>"u"){if(n===Tn.CurrencyDecimal)return e[bt.NumberSymbols][Tn.Decimal];if(n===Tn.CurrencyGroup)return e[bt.NumberSymbols][Tn.Group]}return i}function SD(t,n){return _n(t)[bt.NumberFormats][n]}function DO(t){return _n(t)[bt.Currencies]}function ID(t){if(!t[bt.ExtraData])throw new k(2303,!1)}function kD(t){let n=_n(t);return ID(n),(n[bt.ExtraData][2]||[]).map(i=>typeof i=="string"?Cv(i):[Cv(i[0]),Cv(i[1])])}function MD(t,n,e){let i=_n(t);ID(i);let r=[i[bt.ExtraData][0],i[bt.ExtraData][1]],o=ii(r,n)||[];return ii(o,e)||[]}function ii(t,n){for(let e=n;e>-1;e--)if(typeof t[e]<"u")return t[e];throw new k(2304,!1)}function Cv(t){let[n,e]=t.split(":");return{hours:+n,minutes:+e}}function TD(t,n,e="en"){let i=DO(e)[t]||yD[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var EO=2;function AD(t){let n,e=yD[t];return e&&(n=e[2]),typeof n=="number"?n:EO}var SO=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Tm=Object.create(null),IO=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/,kO=256;function RD(t,n,e,i){let r=BO(t);MO(n),n=Er(e,n)||n;let s=[],a;for(;n;)if(a=IO.exec(n),a){s=s.concat(a.slice(1));let u=s.pop();if(!u)break;n=u}else{s.push(n);break}let c=r.getTimezoneOffset();i&&(c=OD(i,c),r=LO(r,i));let l="";return s.forEach(u=>{let m=PO(u);l+=m?m(r,e,c):u==="''"?"'":u.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function MO(t){if(t.length>kO)throw new k(2300,!1)}function Pm(t,n,e){let i=new Date(0);return i.setFullYear(t,n,e),i.setHours(0,0,0),i}function Er(t,n){let e=CD(t);if(Tm[e]??=Object.create(null),Tm[e][n])return Tm[e][n];let i="";switch(n){case"shortDate":i=al(t,Mn.Short);break;case"mediumDate":i=al(t,Mn.Medium);break;case"longDate":i=al(t,Mn.Long);break;case"fullDate":i=al(t,Mn.Full);break;case"shortTime":i=cl(t,Mn.Short);break;case"mediumTime":i=cl(t,Mn.Medium);break;case"longTime":i=cl(t,Mn.Long);break;case"fullTime":i=cl(t,Mn.Full);break;case"short":let r=Er(t,"shortTime"),o=Er(t,"shortDate");i=Am(ll(t,Mn.Short),[r,o]);break;case"medium":let s=Er(t,"mediumTime"),a=Er(t,"mediumDate");i=Am(ll(t,Mn.Medium),[s,a]);break;case"long":let c=Er(t,"longTime"),l=Er(t,"longDate");i=Am(ll(t,Mn.Long),[c,l]);break;case"full":let u=Er(t,"fullTime"),m=Er(t,"fullDate");i=Am(ll(t,Mn.Full),[u,m]);break}return i&&(Tm[e][n]=i),i}function Am(t,n){return n&&(t=t.replace(/\{([^}]+)}/g,function(e,i){return Object.hasOwn(n,i)?n[i]:e})),t}function wi(t,n,e="-",i,r){let o="";(t<0||r&&t<=0)&&(r?t=-t+1:(t=-t,o=e));let s=String(t);for(;s.length<n;)s="0"+s;return i&&(s=s.slice(s.length-n)),o+s}function TO(t,n){return wi(t,3).substring(0,n)}function Rt(t,n,e=0,i=!1,r=!1){return function(o,s){let a=AO(t,o);if((e>0||a>-e)&&(a+=e),t===3)a===0&&e===-12&&(a=12);else if(t===6)return TO(a,n);let c=Zi(s,Tn.MinusSign);return wi(a,n,c,i,r)}}function AO(t,n){switch(t){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new k(2301,!1)}}function nt(t,n,e=sn.Format,i=!1){return function(r,o){return RO(r,o,t,n,e,i)}}function RO(t,n,e,i,r,o){switch(e){case 2:return DD(n,r,i)[t.getMonth()];case 1:return xD(n,r,i)[t.getDay()];case 0:let s=t.getHours(),a=t.getMinutes();if(o){let l=kD(n),u=MD(n,r,i),m=l.findIndex(g=>{if(Array.isArray(g)){let[_,C]=g,I=s>=_.hours&&a>=_.minutes,j=s<C.hours||s===C.hours&&a<C.minutes;if(_.hours<C.hours){if(I&&j)return!0}else if(I||j)return!0}else if(g.hours===s&&g.minutes===a)return!0;return!1});if(m!==-1)return u[m]}return wD(n,r,i)[s<12?0:1];case 3:return ED(n,i)[t.getFullYear()<=0?0:1];default:let c=e;throw new k(2302,!1)}}function Rm(t){return function(n,e,i){let r=-1*i,o=Zi(e,Tn.MinusSign),s=r>0?Math.floor(r/60):Math.ceil(r/60);switch(t){case 0:return(r>=0?"+":"")+wi(s,2,o)+wi(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+wi(s,1,o);case 2:return"GMT"+(r>=0?"+":"")+wi(s,2,o)+":"+wi(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+wi(s,2,o)+":"+wi(Math.abs(r%60),2,o);default:throw new k(2310,!1)}}}var NO=0,Om=4;function OO(t){let n=Pm(t,NO,1).getDay();return Pm(t,0,1+(n<=Om?Om:Om+7)-n)}function ND(t){let n=t.getDay(),e=n===0?-3:Om-n;return Pm(t.getFullYear(),t.getMonth(),t.getDate()+e)}function wv(t,n=!1){return function(e,i){let r;if(n){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,s=e.getDate();r=1+Math.floor((s+o)/7)}else{let o=ND(e),s=OO(o.getFullYear()),a=o.getTime()-s.getTime();r=1+Math.round(a/6048e5)}return wi(r,t,Zi(i,Tn.MinusSign))}}function Nm(t,n=!1){return function(e,i){let o=ND(e).getFullYear();return wi(o,t,Zi(i,Tn.MinusSign),n)}}var xv=Object.create(null);function PO(t){if(xv[t])return xv[t];let n;switch(t){case"G":case"GG":case"GGG":n=nt(3,We.Abbreviated);break;case"GGGG":n=nt(3,We.Wide);break;case"GGGGG":n=nt(3,We.Narrow);break;case"y":n=Rt(0,1,0,!1,!0);break;case"yy":n=Rt(0,2,0,!0,!0);break;case"yyy":n=Rt(0,3,0,!1,!0);break;case"yyyy":n=Rt(0,4,0,!1,!0);break;case"Y":n=Nm(1);break;case"YY":n=Nm(2,!0);break;case"YYY":n=Nm(3);break;case"YYYY":n=Nm(4);break;case"M":case"L":n=Rt(1,1,1);break;case"MM":case"LL":n=Rt(1,2,1);break;case"MMM":n=nt(2,We.Abbreviated);break;case"MMMM":n=nt(2,We.Wide);break;case"MMMMM":n=nt(2,We.Narrow);break;case"LLL":n=nt(2,We.Abbreviated,sn.Standalone);break;case"LLLL":n=nt(2,We.Wide,sn.Standalone);break;case"LLLLL":n=nt(2,We.Narrow,sn.Standalone);break;case"w":n=wv(1);break;case"ww":n=wv(2);break;case"W":n=wv(1,!0);break;case"d":n=Rt(2,1);break;case"dd":n=Rt(2,2);break;case"c":case"cc":n=Rt(7,1);break;case"ccc":n=nt(1,We.Abbreviated,sn.Standalone);break;case"cccc":n=nt(1,We.Wide,sn.Standalone);break;case"ccccc":n=nt(1,We.Narrow,sn.Standalone);break;case"cccccc":n=nt(1,We.Short,sn.Standalone);break;case"E":case"EE":case"EEE":n=nt(1,We.Abbreviated);break;case"EEEE":n=nt(1,We.Wide);break;case"EEEEE":n=nt(1,We.Narrow);break;case"EEEEEE":n=nt(1,We.Short);break;case"a":case"aa":case"aaa":n=nt(0,We.Abbreviated);break;case"aaaa":n=nt(0,We.Wide);break;case"aaaaa":n=nt(0,We.Narrow);break;case"b":case"bb":case"bbb":n=nt(0,We.Abbreviated,sn.Standalone,!0);break;case"bbbb":n=nt(0,We.Wide,sn.Standalone,!0);break;case"bbbbb":n=nt(0,We.Narrow,sn.Standalone,!0);break;case"B":case"BB":case"BBB":n=nt(0,We.Abbreviated,sn.Format,!0);break;case"BBBB":n=nt(0,We.Wide,sn.Format,!0);break;case"BBBBB":n=nt(0,We.Narrow,sn.Format,!0);break;case"h":n=Rt(3,1,-12);break;case"hh":n=Rt(3,2,-12);break;case"H":n=Rt(3,1);break;case"HH":n=Rt(3,2);break;case"m":n=Rt(4,1);break;case"mm":n=Rt(4,2);break;case"s":n=Rt(5,1);break;case"ss":n=Rt(5,2);break;case"S":n=Rt(6,1);break;case"SS":n=Rt(6,2);break;case"SSS":n=Rt(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Rm(0);break;case"ZZZZZ":n=Rm(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Rm(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Rm(2);break;default:return null}return xv[t]=n,n}function OD(t,n){t=t.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+t)/6e4;return isNaN(e)?n:e}function FO(t,n){return t=new Date(t.getTime()),t.setMinutes(t.getMinutes()+n),t}function LO(t,n,e){let r=t.getTimezoneOffset(),o=OD(n,r);return FO(t,-1*(o-r))}function BO(t){if(_D(t))return t;if(typeof t=="number"&&!isNaN(t))return new Date(t);if(typeof t=="string"){if(t=t.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(t)){let[r,o=1,s=1]=t.split("-").map(a=>+a);return Pm(r,o-1,s)}let e=parseFloat(t);if(!isNaN(t-e))return new Date(e);let i;if(i=t.match(SO))return VO(i)}let n=new Date(t);if(!_D(n))throw new k(2311,!1);return n}function VO(t){let n=new Date(0),e=0,i=0,r=t[8]?n.setUTCFullYear:n.setFullYear,o=t[8]?n.setUTCHours:n.setHours;t[9]&&(e=Number(t[9]+t[10]),i=Number(t[9]+t[11])),r.call(n,Number(t[1]),Number(t[2])-1,Number(t[3]));let s=Number(t[4]||0)-e,a=Number(t[5]||0)-i,c=Number(t[6]||0),l=Math.floor(parseFloat("0."+(t[7]||0))*1e3);return o.call(n,s,a,c,l),n}function _D(t){return t instanceof Date&&!isNaN(t.valueOf())}var jO=/^(\d+)?\.((\d+)(-(\d+))?)?$/,vD=22,Fm=".",dl="0",HO=";",UO=",",Dv="#",bD="\xA4";function zO(t,n,e,i,r,o,s=!1){let a="",c=!1;if(!isFinite(t))a=Zi(e,Tn.Infinity);else{let l=WO(t);s&&(l=GO(l));let u=n.minInt,m=n.minFrac,g=n.maxFrac;if(o){let Ne=o.match(jO);if(Ne===null)throw new k(2306,!1);let ut=Ne[1],Mi=Ne[3],Fr=Ne[5];ut!=null&&(u=Ev(ut)),Mi!=null&&(m=Ev(Mi)),Fr!=null?g=Ev(Fr):Mi!=null&&m>g&&(g=m);let di=100;if(u>di||m>di||g>di)throw new k(2306,!1)}qO(l,m,g);let _=l.digits,C=l.integerLen,I=l.exponent,j=[];for(c=_.every(Ne=>!Ne);C<u;C++)_.unshift(0);for(;C<0;C++)_.unshift(0);C>0?j=_.splice(C,_.length):(j=_,_=[0]);let se=[];for(_.length>=n.lgSize&&se.unshift(_.splice(-n.lgSize,_.length).join(""));_.length>n.gSize;)se.unshift(_.splice(-n.gSize,_.length).join(""));_.length&&se.unshift(_.join("")),a=se.join(Zi(e,i)),j.length&&(a+=Zi(e,r)+j.join("")),I&&(a+=Zi(e,Tn.Exponential)+"+"+I)}return t<0&&!c?a=n.negPre+a+n.negSuf:a=n.posPre+a+n.posSuf,a}function PD(t,n,e,i,r){let o=SD(n,Sv.Currency),s=$O(o,Zi(n,Tn.MinusSign));return s.minFrac=AD(i),s.maxFrac=s.minFrac,zO(t,s,n,Tn.CurrencyGroup,Tn.CurrencyDecimal,r).replace(bD,e).replace(bD,"").trim()}function $O(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(HO),r=i[0],o=i[1],s=r.indexOf(Fm)!==-1?r.split(Fm):[r.substring(0,r.lastIndexOf(dl)+1),r.substring(r.lastIndexOf(dl)+1)],a=s[0],c=s[1]||"";e.posPre=a.substring(0,a.indexOf(Dv));for(let u=0;u<c.length;u++){let m=c.charAt(u);m===dl?e.minFrac=e.maxFrac=u+1:m===Dv?e.maxFrac=u+1:e.posSuf+=m}let l=a.split(UO);if(e.gSize=l[1]?l[1].length:0,e.lgSize=l[2]||l[1]?(l[2]||l[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,m=o.indexOf(Dv);e.negPre=o.substring(0,m).replace(/'/g,""),e.negSuf=o.slice(m+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function GO(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function WO(t){let n=Math.abs(t)+"",e=0,i,r,o,s,a;for((r=n.indexOf(Fm))>-1&&(n=n.replace(Fm,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===dl;o++);if(o===(a=n.length))i=[0],r=1;else{for(a--;n.charAt(a)===dl;)a--;for(r-=o,i=[],s=0;o<=a;o++,s++)i[s]=Number(n.charAt(o))}return r>vD&&(i=i.splice(0,vD-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function qO(t,n,e){if(n>e)throw new k(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),s=o+t.integerLen,a=i[s];if(s>0){i.splice(Math.max(t.integerLen,s));for(let m=s;m<i.length;m++)i[m]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,s=o+1),i[0]=0;for(let m=1;m<s;m++)i[m]=0}if(a>=5)if(s-1<0){for(let m=0;m>s;m--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[s-1]++;for(;r<Math.max(0,o);r++)i.push(0);let c=o!==0,l=n+t.integerLen,u=i.reduceRight(function(m,g,_,C){return g=g+m,C[_]=g<10?g:g-10,c&&(C[_]===0&&_>=l?C.pop():c=!1),g>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function Ev(t){let n=parseInt(t);if(isNaN(n))throw new k(2305,!1);return n}var Qi=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(q);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(re(Vt))};static \u0275dir=T({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[$e]})}return t})();function FD(t,n){return new k(2100,!1)}var YO="mediumDate",LD=new y(""),BD=new y(""),fs=(()=>{class t{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let s=i??this.defaultOptions?.dateFormat??YO,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return RD(e,s,o||this.locale,a)}catch(s){throw FD(t,s.message)}}static \u0275fac=function(i){return new(i||t)(re(_a,16),re(LD,24),re(BD,24))};static \u0275pipe=no({name:"date",type:t,pure:!0})}return t})();var St=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,s){if(!ZO(e))return null;s||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let a=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?a=TD(a,r==="symbol"?"wide":"narrow",s):a=r);try{let c=QO(e);return PD(c,s,a,i,o)}catch(c){throw FD(t,c.message)}}static \u0275fac=function(i){return new(i||t)(re(_a,16),re(hv,16))};static \u0275pipe=no({name:"currency",type:t,pure:!0})}return t})();function ZO(t){return!(t==null||t===""||t!==t)}function QO(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new k(2309,!1);return t}function ul(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Iv="browser";function VD(t){return t===Iv}var ml=class{_doc;constructor(n){this._doc=n}manager},Lm=(()=>{class t extends ml{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(ne(Z))};static \u0275prov=J({token:t,factory:t.\u0275fac})}return t})(),jm=new y(""),Av=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Lm));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Lm);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new k(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(ne(jm),ne(B))};static \u0275prov=J({token:t,factory:t.\u0275fac})}return t})(),kv="ng-app-id";function jD(t){for(let n of t)n.remove()}function HD(t,n){let e=n.createElement("style");return e.textContent=t,e}function tP(t,n,e,i){let r=t.head?.querySelectorAll(`style[${kv}="${n}"],link[${kv}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(kv),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Tv(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Rv=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,tP(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,HD);i?.forEach(r=>this.addUsage(r,this.external,Tv))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(jD(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])jD(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,HD(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Tv(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(ne(Z),ne(pr),ne(Xr,8),ne(ss))};static \u0275prov=J({token:t,factory:t.\u0275fac})}return t})(),Mv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Nv=/%COMP%/g;var zD="%COMP%",nP=`_nghost-${zD}`,iP=`_ngcontent-${zD}`,rP=!0,oP=new y("",{factory:()=>rP});function sP(t){return iP.replace(Nv,t)}function aP(t){return nP.replace(Nv,t)}function $D(t,n){return n.map(e=>e.replace(Nv,t))}var Ov=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new fl(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Vm?r.applyToHost(e):r instanceof hl&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,m=this.tracingService;switch(i.encapsulation){case yi.Emulated:o=new Vm(c,l,i,this.appId,u,s,a,m);break;case yi.ShadowDom:return new Bm(c,e,i,s,a,this.nonce,m,l);case yi.ExperimentalIsolatedShadowDom:return new Bm(c,e,i,s,a,this.nonce,m);default:o=new hl(c,l,i,u,s,a,m);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(ne(Av),ne(us),ne(pr),ne(oP),ne(Z),ne(B),ne(Xr),ne(Yi,8))};static \u0275prov=J({token:t,factory:t.\u0275fac})}return t})(),fl=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Mv[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(UD(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(UD(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new k(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Mv[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Mv[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Wi.DashCase|Wi.Important)?n.style.setProperty(e,i,r&Wi.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Wi.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=ni().getGlobalEventTarget(this.doc,n),!n))throw new k(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function UD(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Bm=class extends fl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=$D(i.id,l);for(let m of l){let g=document.createElement("style");s&&g.setAttribute("nonce",s),g.textContent=m,this.shadowRoot.appendChild(g)}let u=i.getExternalStyles?.();if(u)for(let m of u){let g=Tv(m,r);s&&g.setAttribute("nonce",s),this.shadowRoot.appendChild(g)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},hl=class extends fl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?$D(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&eo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Vm=class extends hl{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=sP(l),this.hostAttr=aP(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Hm=class t extends sl{supportsDOMEvents=!0;static makeCurrent(){yv(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=cP();return e==null?null:lP(e)}resetBaseElement(){pl=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return ul(document.cookie,n)}},pl=null;function cP(){return pl=pl||document.head.querySelector("base"),pl?pl.getAttribute("href"):null}function lP(t){return new URL(t,document.baseURI).pathname}var GD=["alt","control","meta","shift"],dP={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},uP={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},WD=(()=>{class t extends ml{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>ni().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),GD.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=dP[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),GD.forEach(s=>{if(s!==r){let a=uP[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(ne(Z))};static \u0275prov=J({token:t,factory:t.\u0275fac})}return t})();async function Pv(t,n,e){let i=b({rootComponent:t},mP(n,e));return dD(i)}function mP(t,n){return{platformRef:n?.platformRef,appProviders:[..._P,...t?.providers??[]],platformProviders:gP}}function fP(){Hm.makeCurrent()}function hP(){return new Jt}function pP(){return D_(document),document}var gP=[{provide:ss,useValue:Iv},{provide:ku,useValue:fP,multi:!0},{provide:Z,useFactory:pP}];var _P=[{provide:Mc,useValue:"root"},{provide:Jt,useFactory:hP},{provide:jm,useClass:Lm,multi:!0},{provide:jm,useClass:WD,multi:!0},Ov,{provide:us,useClass:Rv},{provide:Rv,useExisting:us},Av,{provide:At,useExisting:Ov},[]];var Sr=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var zm=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},$m=class{encodeKey(n){return qD(n)}encodeValue(n){return qD(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function vP(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var bP=/%(\d[a-f0-9])/gi,yP={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function qD(t){return encodeURIComponent(t).replace(bP,(n,e)=>yP[e]??n)}function Um(t){return`${t}`}var Bn=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new $m,n.fromString){if(n.fromObject)throw new k(2805,!1);this.map=vP(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Um):[Um(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Um(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Um(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function CP(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function YD(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function ZD(t){return typeof Blob<"u"&&t instanceof Blob}function QD(t){return typeof FormData<"u"&&t instanceof FormData}function wP(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Fv="Content-Type",KD="Accept",JD="text/plain",eE="application/json",xP=`${eE}, ${JD}, */*`,ya=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(CP(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new k(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Sr,this.context??=new zm,!this.params)this.params=new Bn,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let u=a.indexOf("?"),m=u===-1?"?":u<a.length-1?"&":"";this.urlWithParams=a+m+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||YD(this.body)||ZD(this.body)||QD(this.body)||wP(this.body)?this.body:this.body instanceof Bn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||QD(this.body)?null:ZD(this.body)?this.body.type||null:YD(this.body)?null:typeof this.body=="string"?JD:this.body instanceof Bn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?eE:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,u=n.credentials||this.credentials,m=n.referrer??this.referrer,g=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,C=n.transferCache??this.transferCache,I=n.timeout??this.timeout,j=n.body!==void 0?n.body:this.body,se=n.withCredentials??this.withCredentials,Ne=n.reportProgress??this.reportProgress,ut=n.reportUploadProgress??this.reportUploadProgress,Mi=n.reportDownloadProgress??this.reportDownloadProgress,Fr=n.headers||this.headers,di=n.params||this.params,bd=n.context??this.context;return n.setHeaders!==void 0&&(Fr=Object.keys(n.setHeaders).reduce((Fs,Ao)=>Fs.set(Ao,n.setHeaders[Ao]),Fr)),n.setParams&&(di=Object.keys(n.setParams).reduce((Fs,Ao)=>Fs.set(Ao,n.setParams[Ao]),di)),new t(e,i,j,{params:di,headers:Fr,context:bd,reportProgress:Ne,reportUploadProgress:ut,reportDownloadProgress:Mi,responseType:r,withCredentials:se,transferCache:C,keepalive:o,cache:a,priority:s,timeout:I,mode:c,redirect:l,credentials:u,referrer:m,integrity:g,referrerPolicy:_})}},Ca=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Ca||{}),wa=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Sr,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Gm=class t extends wa{constructor(n={}){super(n)}type=Ca.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},gl=class t extends wa{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Ca.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},hs=class extends wa{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},DP=200;var EP=/^\)\]\}',?\n/,kK=1024*1024,tE=new y("",{factory:()=>null}),Wm=(()=>{class t{fetchImpl=d(Bv,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=d(B);destroyRef=d(Lt);maxResponseSize=d(tE);handle(e){return new le(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(Vv,s=>i.error(new hs({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}async doRequest(e,i,r){let o=this.createRequestInit(e),s;try{let j=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,b({signal:i},o)));SP(j),r.next({type:Ca.Sent}),s=await j}catch(j){r.error(new hs({error:j,status:j.status??0,statusText:j.statusText,url:e.urlWithParams,headers:j.headers}));return}let a=new Sr(s.headers),c=s.statusText,l=s.url||e.urlWithParams,u=s.status,m=null,g=e.reportProgress||e.reportDownloadProgress;if(g&&r.next(new Gm({headers:a,status:u,statusText:c,url:l})),s.body){let j=s.headers.get("content-length"),se=j!==null?Number(j):NaN;this.maxResponseSize!==null&&Number.isFinite(se)&&se>this.maxResponseSize&&XD(this.maxResponseSize);let Ne=[],ut=s.body.getReader(),Mi=0,Fr,di,bd=typeof Zone<"u"&&Zone.current,Fs=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await ut.cancel(),Fs=!0;break}let{done:oc,value:ap}=await ut.read();if(oc)break;if(Ne.push(ap),Mi+=ap.length,this.maxResponseSize!==null&&Mi>this.maxResponseSize&&(await ut.cancel(),XD(this.maxResponseSize)),g){di=e.responseType==="text"?(di??"")+(Fr??=new TextDecoder).decode(ap,{stream:!0}):void 0;let Ay=()=>r.next({type:Ca.DownloadProgress,total:Number.isFinite(se)?se:void 0,loaded:Mi,partialText:di});bd?bd.run(Ay):Ay()}}}),Fs){r.complete();return}let Ao=this.concatChunks(Ne,Mi);try{let oc=s.headers.get(Fv)??"";m=this.parseBody(e,Ao,oc,u)}catch(oc){r.error(new hs({error:oc,headers:new Sr(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}u===0&&(u=m?DP:0);let _=u>=200&&u<300,C=s.redirected,I=s.type;_?(r.next(new gl({body:m,headers:a,status:u,statusText:c,url:l,redirected:C,responseType:I})),r.complete()):r.error(new hs({error:m,headers:a,status:u,statusText:c,url:l,redirected:C,responseType:I}))}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(EP,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new k(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(KD)||(i[KD]=xP),!e.headers.has(Fv)){let o=e.detectContentTypeHeader();o!==null&&(i[Fv]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Bv=class{};function Vv(){}function SP(t){t.then(Vv,Vv)}function XD(t){throw new k(-2825,!1)}function IP(t,n){return n(t)}function kP(t,n,e){return(i,r)=>zt(e,()=>n(i,o=>t(o,r)))}var jv=new y("",{factory:()=>[]}),nE=new y(""),iE=new y("",{factory:()=>!0});var Hv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(Wm),r},providedIn:"root"})}return t})();var qm=(()=>{class t{backend;injector;chain=null;pendingTasks=d(Fc);contributeToStability=d(iE);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(jv),...this.injector.get(nE,[])]));this.chain=r.reduceRight((o,s)=>kP(o,s,this.injector),IP)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return Ce(()=>i(e,o=>this.backend.handle(o))).pipe(Go(r))}else return Ce(()=>i(e,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||t)(ne(Hv),ne(Ue))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Uv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(qm),r},providedIn:"root"})}return t})();function Lv(t,n){return b({body:n},t)}var an=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof ya)o=e;else{let c;r.headers instanceof Sr?c=r.headers:c=new Sr(r.headers);let l;r.params&&(r.params instanceof Bn?l=r.params:l=new Bn({fromObject:r.params})),o=new ya(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=W(o).pipe(zo(c=>this.handler.handle(c)));if(e instanceof ya||r.observe==="events")return s;let a=s.pipe(Ie(c=>c instanceof gl));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(Y(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new k(2806,!1);return c.body}));case"blob":return a.pipe(Y(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new k(2807,!1);return c.body}));case"text":return a.pipe(Y(c=>{if(c.body!==null&&typeof c.body!="string")throw new k(2808,!1);return c.body}));default:return a.pipe(Y(c=>c.body))}case"response":return a;default:throw new k(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Bn().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Lv(r,i))}post(e,i,r={}){return this.request("POST",e,Lv(r,i))}put(e,i,r={}){return this.request("PUT",e,Lv(r,i))}static \u0275fac=function(i){return new(i||t)(ne(Uv))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var MP=new y("",{factory:()=>!0}),TP="XSRF-TOKEN",AP=new y("",{factory:()=>TP}),RP="X-XSRF-TOKEN",NP=new y("",{factory:()=>RP}),OP=(()=>{class t{cookieName=d(AP);doc=d(Z);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=ul(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),rE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(OP),r},providedIn:"root"})}return t})();function PP(t,n){if(!d(MP)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(va).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(rE).getToken(),i=d(NP);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var zv=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t[t.Xhr=7]="Xhr",t})(zv||{});function FP(t,n){return{\u0275kind:t,\u0275providers:n}}function $v(...t){let n=[an,Wm,qm,{provide:Uv,useExisting:qm},{provide:Hv,useFactory:()=>d(Wm)},{provide:jv,useValue:PP,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return pi(n)}function Gv(t){return FP(zv.Interceptors,t.map(n=>({provide:jv,useValue:n,multi:!0})))}var oE=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(ne(Z))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _l=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=J({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(BP),r},providedIn:"root"})}return t})(),BP=(()=>{class t extends _l{_doc=d(Z);sanitize(e,i){if(i==null)return null;switch(e){case Bt.NONE:return i;case Bt.HTML:return _r(i,"HTML")?Ci(i):A_(this._doc,String(i)).toString();case Bt.STYLE:return _r(i,"Style")?Ci(i):i;case Bt.SCRIPT:if(_r(i,"Script"))return Ci(i);throw new k(5200,!1);case Bt.URL:return _r(i,"URL")?Ci(i):Zc(String(i));case Bt.RESOURCE_URL:if(_r(i,"ResourceURL"))return Ci(i);throw new k(-5201,!1);default:throw new k(5202,!1)}}bypassSecurityTrustHtml(e){return S_(e)}bypassSecurityTrustStyle(e){return I_(e)}bypassSecurityTrustScript(e){return k_(e)}bypassSecurityTrustUrl(e){return M_(e)}bypassSecurityTrustResourceUrl(e){return T_(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var me="primary",Rl=Symbol("RouteTitle"),Qv=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function gs(t){return new Qv(t)}function Wv(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function hE(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return Wv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!Wv(o,t.slice(0,o.length),a)||!Wv(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function Jm(t){return new Promise((n,e)=>{t.pipe(dr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function VP(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Ki(t[e],n[e]))return!1;return!0}function Ki(t,n){let e=t?Kv(t):void 0,i=n?Kv(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!pE(t[r],n[r]))return!1;return!0}function Kv(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function pE(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function jP(t){return t.length>0?t[t.length-1]:null}function bs(t){return gc(t)?t:yr(t)?ot(Promise.resolve(t)):W(t)}function gE(t){return gc(t)?Jm(t):Promise.resolve(t)}var HP={exact:vE,subset:bE},_E={exact:UP,subset:zP,ignored:()=>!0},ub={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},xl={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function mb(t,n,e){let i=t instanceof bn?t:n.parseUrl(t);return dt(()=>Xv(n.lastSuccessfulNavigation()?.finalUrl??new bn,i,b(b({},xl),e)))}function Xv(t,n,e){return HP[e.paths](t.root,n.root,e.matrixParams)&&_E[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function UP(t,n){return Ki(t,n)}function vE(t,n,e){if(!ps(t.segments,n.segments)||!Qm(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!vE(t.children[i],n.children[i],e))return!1;return!0}function zP(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>pE(t[e],n[e]))}function bE(t,n,e){return yE(t,n,n.segments,e)}function yE(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!ps(r,e)||n.hasChildren()||!Qm(r,e,i))}else if(t.segments.length===e.length){if(!ps(t.segments,e)||!Qm(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!bE(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!ps(t.segments,r)||!Qm(t.segments,r,i)||!t.children[me]?!1:yE(t.children[me],n,o,i)}}function Qm(t,n,e){return n.every((i,r)=>_E[e](t[r].parameters,i.parameters))}var bn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Pe([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=gs(this.queryParams),this._queryParamMap}toString(){return WP.serialize(this)}},Pe=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Km(this)}},so=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=gs(this.parameters),this._parameterMap}toString(){return wE(this)}};function $P(t,n){return ps(t,n)&&t.every((e,i)=>Ki(e.parameters,n[i].parameters))}function ps(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function GP(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===me&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==me&&(e=e.concat(n(r,i)))}),e}var Aa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:()=>new ao})}return t})(),ao=class{parse(n){let e=new eb(n);return new bn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${vl(n.root,!0)}`,i=ZP(n.queryParams),r=typeof n.fragment=="string"?`#${qP(n.fragment)}`:"";return`${e}${i}${r}`}},WP=new ao;function Km(t){return t.segments.map(n=>wE(n)).join("/")}function vl(t,n){if(!t.hasChildren())return Km(t);if(n){let e=t.children[me]?vl(t.children[me],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==me&&i.push(`${r}:${vl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=GP(t,(i,r)=>r===me?[vl(t.children[me],!1)]:[`${r}:${vl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[me]!=null?`${Km(t)}/${e[0]}`:`${Km(t)}/(${e.join("//")})`}}function CE(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Ym(t){return CE(t).replace(/%3B/gi,";")}function qP(t){return encodeURI(t)}function Jv(t){return CE(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Xm(t){return decodeURIComponent(t)}function aE(t){return Xm(t.replace(/\+/g,"%20"))}function wE(t){return`${Jv(t.path)}${YP(t.parameters)}`}function YP(t){return Object.entries(t).map(([n,e])=>`;${Jv(n)}=${Jv(e)}`).join("")}function ZP(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Ym(e)}=${Ym(r)}`).join("&"):`${Ym(e)}=${Ym(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var QP=/^[^\/()?;#]+/;function qv(t){let n=t.match(QP);return n?n[0]:""}var KP=/^[^\/()?;=#]+/;function XP(t){let n=t.match(KP);return n?n[0]:""}var JP=/^[^=?&#]+/;function eF(t){let n=t.match(JP);return n?n[0]:""}var tF=/^[^&#]+/;function nF(t){let n=t.match(tF);return n?n[0]:""}var eb=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Pe([],{}):new Pe([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new k(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[me]=new Pe(e,i)),r}parseSegment(){let n=qv(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new k(4009,!1);return this.capture(n),new so(Xm(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=XP(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=qv(this.remaining);r&&(i=r,this.capture(i))}n[Xm(e)]=Xm(i)}parseQueryParam(n){let e=eF(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=nF(this.remaining);s&&(i=s,this.capture(i))}let r=aE(e),o=aE(i);if(Object.hasOwn(n,r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=qv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new k(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=me);let a=this.parseChildren(e+1);i[s??me]=Object.keys(a).length===1&&a[me]?a[me]:new Pe([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new k(4011,!1)}};function xE(t){return t.segments.length>0?new Pe([],{[me]:t}):t}function DE(t){let n=Object.create(null);for(let[i,r]of Object.entries(t.children)){let o=DE(r);if(i===me&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Pe(t.segments,n);return iF(e)}function iF(t){if(t.numberOfChildren===1&&t.children[me]){let n=t.children[me];return new Pe(t.segments.concat(n.segments),n.children)}return t}function co(t){return t instanceof bn}function EE(t,n,e=null,i=null,r=new ao){let o=SE(t);return IE(o,n,e,i,r)}function SE(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new Pe(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=xE(i);return n??r}function IE(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Yv(o,o,o,e,i,r);let s=rF(n);if(s.toRoot())return Yv(o,o,new Pe([],{}),e,i,r);let a=oF(s,o,t),c=a.processChildren?yl(a.segmentGroup,a.index,s.commands):ME(a.segmentGroup,a.index,s.commands);return Yv(o,a.segmentGroup,c,e,i,r)}function ef(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Dl(t){return typeof t=="object"&&t!=null&&t.outlets}function cE(t,n,e){t||="\u0275";let i=new bn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Yv(t,n,e,i,r,o){let s={};for(let[l,u]of Object.entries(i??{}))s[l]=Array.isArray(u)?u.map(m=>cE(l,m,o)):cE(l,u,o);let a;t===n?a=e:a=kE(t,n,e);let c=xE(DE(a));return new bn(c,s,r)}function kE(t,n,e){let i=Object.create(null);return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=kE(o,n,e)}),new Pe(t.segments,i)}var tf=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&ef(i[0]))throw new k(4003,!1);let r=i.find(Dl);if(r&&r!==jP(i))throw new k(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function rF(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new tf(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new tf(e,n,i)}var Da=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function oF(t,n,e){if(t.isAbsolute)return new Da(n,!0,0);if(!e)return new Da(n,!1,NaN);if(e.parent===null)return new Da(e,!0,0);let i=ef(t.commands[0])?0:1,r=e.segments.length-1+i;return sF(e,r,t.numberOfDoubleDots)}function sF(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new k(4005,!1);r=i.segments.length}return new Da(i,!1,r-o)}function aF(t){return Dl(t[0])?t[0].outlets:{[me]:t}}function ME(t,n,e){if(t??=new Pe([],{}),t.segments.length===0&&t.hasChildren())return yl(t,n,e);let i=cF(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Pe(t.segments.slice(0,i.pathIndex),{});return o.children[me]=new Pe(t.segments.slice(i.pathIndex),t.children),yl(o,0,r)}else return i.match&&r.length===0?new Pe(t.segments,{}):i.match&&!t.hasChildren()?tb(t,n,e):i.match?yl(t,0,r):tb(t,n,e)}function yl(t,n,e){if(e.length===0)return new Pe(t.segments,{});{let i=aF(e),r=Object.create(null);if(Object.keys(i).some(o=>o!==me)&&t.children[me]&&t.numberOfChildren===1&&t.children[me].segments.length===0){let o=yl(t.children[me],n,e);return new Pe(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=ME(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Pe(t.segments,r)}}function cF(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(Dl(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!dE(c,l,s))return o;i+=2}else{if(!dE(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function tb(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Dl(o)){let c=lF(o.outlets);return new Pe(i,c)}if(r===0&&ef(e[0])){let c=t.segments[n];i.push(new so(c.path,lE(e[0]))),r++;continue}let s=Dl(o)?o.outlets[me]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&ef(a)?(i.push(new so(s,lE(a))),r+=2):(i.push(new so(s,{})),r++)}return new Pe(i,{})}function lF(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=tb(new Pe([],{}),0,i))}),n}function lE(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function dE(t,n,e){return t==e.path&&Ki(n,e.parameters)}var Cl="imperative",Gt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Gt||{}),jn=class{id;url;constructor(n,e){this.id=n,this.url=e}},_s=class extends jn{type=Gt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Di=class extends jn{urlAfterRedirects;type=Gt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},cn=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(cn||{}),El=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(El||{}),ri=class extends jn{reason;code;type=Gt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function TE(t){return t instanceof ri&&(t.code===cn.Redirect||t.code===cn.SupersededByNewNavigation)}var kr=class extends jn{reason;code;type=Gt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},vs=class extends jn{error;target;type=Gt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Sl=class extends jn{urlAfterRedirects;state;type=Gt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},nf=class extends jn{urlAfterRedirects;state;type=Gt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},rf=class extends jn{urlAfterRedirects;state;shouldActivate;type=Gt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},of=class extends jn{urlAfterRedirects;state;type=Gt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},sf=class extends jn{urlAfterRedirects;state;type=Gt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},af=class{route;type=Gt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},cf=class{route;type=Gt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},lf=class{snapshot;type=Gt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},df=class{snapshot;type=Gt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},uf=class{snapshot;type=Gt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},mf=class{snapshot;type=Gt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Sa=class{},Il=class{},Ia=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function dF(t){return!(t instanceof Sa)&&!(t instanceof Ia)&&!(t instanceof Il)}var ff=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Ra(this.rootInjector)}},Ra=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new ff(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(ne(Ue))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),hf=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=nb(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=nb(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=ib(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return ib(n,this._root).map(e=>e.value)}};function nb(t,n){if(t===n.value)return n;for(let e of n.children){let i=nb(t,e);if(i)return i}return null}function ib(t,n){if(t===n.value)return[n];for(let e of n.children){let i=ib(t,e);if(i.length)return i.unshift(n),i}return[]}var Vn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function xa(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var kl=class extends hf{snapshot;constructor(n,e){super(n),this.snapshot=e,hb(this,n)}toString(){return this.snapshot.toString()}};function AE(t,n){let e=uF(t,n),i=new Pt([new so("",{})]),r=new Pt({}),o=new Pt({}),s=new Pt({}),a=new Pt(""),c=new yn(i,r,s,a,o,me,t,e.root);return c.snapshot=e.root,new kl(new Vn(c,[]),e)}function uF(t,n){let e={},i={},r={},s=new ka([],e,r,"",i,me,t,null,{},n);return new Ml("",new Vn(s,[]))}var yn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Y(l=>l[Rl]))??W(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Y(n=>gs(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Y(n=>gs(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},mF="always";function fb(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:b(b({},n.params),t.params),data:b(b({},n.data),t.data),resolve:b(b(b(b({},t.data),n.data),r?.data),t._resolvedData)}:i={params:b({},t.params),data:b({},t.data),resolve:b(b({},t.data),t._resolvedData??{})},r&&NE(r)&&(i.resolve[Rl]=r.title),i}var ka=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Rl]}constructor(n,e,i,r,o,s,a,c,l,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=gs(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=gs(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},Ml=class extends hf{url;constructor(n,e){super(e),this.url=n,hb(this,e)}toString(){return RE(this._root)}};function hb(t,n){n.value._routerState=t,n.children.forEach(e=>hb(t,e))}function RE(t){let n=t.children.length>0?` { ${t.children.map(RE).join(", ")} } `:"";return`${t.value}${n}`}function Zv(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Ki(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Ki(n.params,e.params)||t.paramsSubject.next(e.params),VP(n.url,e.url)||t.urlSubject.next(e.url),Ki(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function rb(t,n){let e=Ki(t.params,n.params)&&$P(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||rb(t.parent,n.parent))}function NE(t){return typeof t.title=="string"||t.title===null}var OE=new y(""),Nl=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=me;activateEvents=new U;deactivateEvents=new U;attachEvents=new U;detachEvents=new U;routerOutletData=vn();parentContexts=d(Ra);location=d(Vt);changeDetector=d(ge);inputBinder=d(vf,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new k(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new k(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new k(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new k(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new ob(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[$e]})}return t})(),ob=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===yn?this.route:n===Ra?this.childContexts:n===OE?this.outletData:this.parent.get(n,e)}},vf=new y("");var pb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&M(0,"router-outlet")},dependencies:[Nl],encapsulation:2,changeDetection:1})}return t})();function gb(t){let n=t.children&&t.children.map(gb),e=n?Q(b({},t),{children:n}):b({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==me&&(e.component=pb),e}function fF(t,n,e){let i=new Set,r=Tl(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new kl(r,n)}}function Tl(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=hF(t,n,e,i);return new Vn(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(c=>Tl(t,c,void 0,i)),a}}let r=pF(n.value);i.add(r);let o=n.children.map(s=>Tl(t,s,void 0,i));return new Vn(r,o)}}function hF(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return Tl(t,r,o,i);return Tl(t,r,void 0,i)})}function pF(t){return new yn(new Pt(t.url),new Pt(t.params),new Pt(t.queryParams),new Pt(t.fragment),new Pt(t.data),t.outlet,t.component,t)}var Ma=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},PE="ngNavigationCancelingError";function pf(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=co(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=FE(!1,cn.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function FE(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[PE]=!0,e.cancellationCode=n,e}function gF(t){return LE(t)&&co(t.url)}function LE(t){return!!t&&t[PE]}var sb=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Zv(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=xa(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=xa(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=xa(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=xa(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new mf(o.value.snapshot))}),n.children.length&&this.forwardEvent(new df(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Zv(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Zv(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},gf=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Ea=class{component;route;constructor(n,e){this.component=n,this.route=e}};function _F(t,n,e){let i=t._root,r=n?n._root:null;return bl(i,r,e,[i.value])}function vF(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Na(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Up(t)?t:n.get(t):i}function bl(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=xa(n);return t.children.forEach(s=>{bF(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>wl(a,e.getContext(s),r)),r}function bF(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=yF(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new gf(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?bl(t,n,a?a.children:null,i,r):bl(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new Ea(a.outlet.component,s))}else s&&wl(n,a,r),r.canActivateChecks.push(new gf(i)),o.component?bl(t,null,a?a.children:null,i,r):bl(t,null,e,i,r);return r}function yF(t,n,e){if(typeof e=="function")return zt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!ps(t.url,n.url);case"pathParamsOrQueryParamsChange":return!ps(t.url,n.url)||!Ki(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!rb(t,n)||!Ki(t.queryParams,n.queryParams);default:return!rb(t,n)}}function wl(t,n,e){let i=xa(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?wl(s,n.children.getContext(o),e):wl(s,null,e):wl(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Ea(n.outlet.component,r)):e.canDeactivateChecks.push(new Ea(null,r)):e.canDeactivateChecks.push(new Ea(null,r))}function Ol(t){return typeof t=="function"}function CF(t){return typeof t=="boolean"}function wF(t){return t&&Ol(t.canLoad)}function xF(t){return t&&Ol(t.canActivate)}function DF(t){return t&&Ol(t.canActivateChild)}function EF(t){return t&&Ol(t.canDeactivate)}function SF(t){return t&&Ol(t.canMatch)}function BE(t){return t instanceof fi||t?.name==="EmptyError"}var Zm=Symbol("INITIAL_VALUE");function Ta(){return pt(t=>_c(t.map(n=>n.pipe(mt(1),et(Zm)))).pipe(Y(n=>{for(let e of n)if(e!==!0){if(e===Zm)return Zm;if(e===!1||IF(e))return e}return!0}),Ie(n=>n!==Zm),mt(1)))}function IF(t){return co(t)||t instanceof Ma}function VE(t){return t.aborted?W(void 0).pipe(mt(1)):new le(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function jE(t){return pe(VE(t))}function kF(t){return Kt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?W(Q(b({},n),{guardsResult:!0})):MF(o,e,i).pipe(Kt(s=>s&&CF(s)?TF(e,r,t):W(s)),Y(s=>Q(b({},n),{guardsResult:s})))})}function MF(t,n,e){return ot(t).pipe(Kt(i=>PF(i.component,i.route,e,n)),dr(i=>i!==!0,!0))}function TF(t,n,e){return ot(n).pipe(zo(i=>Ur(RF(i.route.parent,e),AF(i.route,e),OF(t,i.path),NF(t,i.route))),dr(i=>i!==!0,!0))}function AF(t,n){return t!==null&&n&&n(new uf(t)),W(!0)}function RF(t,n){return t!==null&&n&&n(new lf(t)),W(!0)}function NF(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return W(!0);let i=e.map(r=>hi(()=>{let o=n._environmentInjector,s=Na(r,o),a=xF(s)?s.canActivate(n,t):zt(o,()=>s(n,t));return bs(a).pipe(dr())}));return W(i).pipe(Ta())}function OF(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>vF(o)).filter(o=>o!==null).map(o=>hi(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Na(a,c),u=DF(l)?l.canActivateChild(e,t):zt(c,()=>l(e,t));return bs(u).pipe(dr())});return W(s).pipe(Ta())}));return W(r).pipe(Ta())}function PF(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return W(!0);let o=r.map(s=>{let a=n._environmentInjector,c=Na(s,a),l=EF(c)?c.canDeactivate(t,n,e,i):zt(a,()=>c(t,n,e,i));return bs(l).pipe(dr())});return W(o).pipe(Ta())}function FF(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return W(!0);let s=o.map(a=>{let c=Na(a,t),l=wF(c)?c.canLoad(n,e):zt(t,()=>c(n,e)),u=bs(l);return r?u.pipe(jE(r)):u});return W(s).pipe(Ta(),HE(i))}function HE(t){return wp(tt(n=>{if(typeof n!="boolean")throw pf(t,n)}),Y(n=>n===!0))}function LF(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return W(!0);let a=s.map(c=>{let l=Na(c,t),u=SF(l)?l.canMatch(n,e,r):zt(t,()=>l(n,e,r));return bs(u).pipe(jE(o))});return W(a).pipe(Ta(),HE(i))}var Ir=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Al=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function BF(t){throw new k(4e3,!1)}function VF(t){throw FE(!1,cn.GuardRejected)}var ab=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[me])throw BF(`${n.redirectTo}`);r=r.children[me]}}async applyRedirectCommands(n,e,i,r,o){let s=await jF(e,r,o);if(s instanceof bn)throw new Al(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Al(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new bn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s=Object.create(null);return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new Pe(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new k(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function jF(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Jm(bs(zt(e,()=>i(n))))}function HF(t,n){return t.providers&&!t._injector&&(t._injector=tl(t.providers,n,`Route: ${t.path}`)),t._injector??n}function xi(t){return t.outlet||me}function UF(t,n){let e=t.filter(i=>xi(i)===n);return e.push(...t.filter(i=>xi(i)!==n)),e}var cb={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function UE(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function zF(t,n,e,i,r,o,s){let a=zE(t,n,e);if(!a.matched)return W(a);let c=UE(o(a));return i=HF(n,i),LF(i,n,e,r,c,s).pipe(Y(l=>l===!0?a:b({},cb)))}function zE(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?b({},cb):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||hE)(e,t,n);if(!r)return b({},cb);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?b(b({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function uE(t,n,e,i,r){return e.length>0&&WF(t,e,i,r)?{segmentGroup:new Pe(n,GF(i,new Pe(e,t.children))),slicedSegments:[]}:e.length===0&&qF(t,e,i)?{segmentGroup:new Pe(t.segments,$F(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Pe(t.segments,t.children),slicedSegments:e}}function $F(t,n,e,i){let r={};for(let o of e)if(bf(t,n,o)&&!i[xi(o)]){let s=new Pe([],{});r[xi(o)]=s}return b(b({},i),r)}function GF(t,n){let e={};e[me]=n;for(let i of t)if(i.path===""&&xi(i)!==me){let r=new Pe([],{});e[xi(i)]=r}return e}function WF(t,n,e,i){return e.some(r=>!bf(t,n,r)||!(xi(r)!==me)?!1:!(i!==void 0&&xi(r)===i))}function qF(t,n,e){return e.some(i=>bf(t,n,i))}function bf(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function YF(t,n,e){return n.length===0&&!t.children[e]}var lb=class{};async function ZF(t,n,e,i,r,o,s,a){return new db(t,n,e,i,r,s,o,a).recognize()}var QF=31,db=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new ab(this.urlSerializer,this.urlTree)}noMatchError(n){return new k(4002,`'${n.segmentGroup}'`)}async recognize(){let n=uE(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Vn(i,e),o=new Ml("",r),s=EE(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new ka([],Object.freeze({}),Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),me,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,me,e),rootSnapshot:e}}catch(i){if(i instanceof Al)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ir?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof Vn?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],u=UF(e,c),m=await this.processSegmentGroup(n,u,l,c,r);s.push(...m)}let a=$E(s);return KF(a),a}async processSegment(n,e,i,r,o,s,a){for(let c of e)try{return await this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof Ir||BE(l))continue;throw l}if(YF(i,r,o))return new lb;throw new Ir(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,c){if(xi(i)!==s&&(s===me||!bf(r,o,i)))throw new Ir(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new Ir(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:m,remainingSegments:g}=zE(e,r,o);if(!c)throw new Ir(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>QF&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let C=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,m,UE(_),n),I=await this.applyRedirects.lineralizeSegments(r,C);return this.processSegment(n,i,e,I.concat(g),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new ka(i,r,Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,JF(e),xi(e),e.component??e._loadedComponent??null,e,eL(e),n),a=fb(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=ut=>this.createSnapshot(n,i,ut.consumedSegments,ut.parameters,s),c=await Jm(zF(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new Ir(e);n=i._injector??n;let{routes:l}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:m,consumedSegments:g,remainingSegments:_}=c,C=this.createSnapshot(n,i,g,m,s),{segmentGroup:I,slicedSegments:j}=uE(e,g,_,l,o);if(j.length===0&&I.hasChildren()){let ut=await this.processChildren(u,l,I,C);return new Vn(C,ut)}if(l.length===0&&j.length===0)return new Vn(C,[]);let se=xi(i)===o,Ne=await this.processSegment(u,l,I,j,se?me:o,!0,C);return new Vn(C,Ne instanceof Vn?[Ne]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Jm(FF(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw VF(e)}return{routes:[],injector:n}}};function KF(t){t.sort((n,e)=>n.value.outlet===me?-1:e.value.outlet===me?1:n.value.outlet.localeCompare(e.value.outlet))}function XF(t){let n=t.value.routeConfig;return n&&n.path===""}function $E(t){let n=[],e=new Set;for(let i of t){if(!XF(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=$E(i.children);n.push(new Vn(i.value,r))}return n.filter(i=>!e.has(i))}function JF(t){return t.data||{}}function eL(t){return t.resolve||{}}function tL(t,n,e,i,r,o,s){return Kt(async a=>{let{state:c,tree:l}=await ZF(t,n,e,i,a.extractedUrl,r,o,s);return Q(b({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function nL(t){return Kt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return W(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of GE(a))o.add(c);let s=0;return ot(o).pipe(zo(a=>r.has(a)?iL(a,e,t):(a.data=fb(a,a.parent,t).resolve,W(void 0))),tt(()=>s++),Kd(1),Kt(a=>s===o.size?W(n):xt))})}function GE(t){let n=t.children.map(e=>GE(e)).flat();return[t,...n]}function iL(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!NE(i)&&(r[Rl]=i.title),hi(()=>(t.data=fb(t,t.parent,e).resolve,rL(r,t,n).pipe(Y(o=>(t._resolvedData=o,t.data=b(b({},t.data),o),null)))))}function rL(t,n,e){let i=Kv(t);if(i.length===0)return W({});let r={};return ot(i).pipe(Kt(o=>oL(t[o],n,e).pipe(dr(),tt(s=>{if(s instanceof Ma)throw pf(new ao,s);r[o]=s}))),Kd(1),Y(()=>r),zr(o=>BE(o)?xt:pc(o)))}function oL(t,n,e){let i=n._environmentInjector,r=Na(t,i),o=r.resolve?r.resolve(n,e):zt(i,()=>r(n,e));return bs(o)}function mE(t){return pt(n=>{let e=t(n);return e?ot(e).pipe(Y(()=>n)):W(n)})}var _b=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===me);return i}getResolvedTitleForRoute(e){return e.data[Rl]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:()=>d(WE)})}return t})(),WE=(()=>{class t extends _b{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(ne(oE))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Oa=new y("",{factory:()=>({})}),Pl=new y(""),qE=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(fv);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await gE(zt(e,()=>i.loadComponent())),s=await ZE(vv(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await YE(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();async function YE(t,n,e,i){let r=await gE(zt(e,()=>t.loadChildren())),o=await ZE(vv(r)),s;o instanceof pm||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,u;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,u=s,c=a.get(Pl,[],{optional:!0,self:!0}).flat()),{routes:c.map(gb),injector:a,factory:u}}async function ZE(t){return t}var yf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:()=>d(sL)})}return t})(),sL=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),QE=new y("");var KE=new y(""),aL=()=>{},XE=new y(""),JE=(()=>{class t{currentNavigation=V(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=V(null);events=new S;transitionAbortWithErrorSubject=new S;configLoader=d(qE);environmentInjector=d(Ue);destroyRef=d(Lt);urlSerializer=d(Aa);rootContexts=d(Ra);location=d(oo);inputBindingEnabled=d(vf,{optional:!0})!==null;titleStrategy=d(_b);options=d(Oa,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||mF;urlHandlingStrategy=d(yf);createViewTransition=d(QE,{optional:!0});navigationErrorHandler=d(XE,{optional:!0});activatedRouteInjectorFeature=d(KE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>W(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new af(r)),i=r=>this.events.next(new cf(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Ce(()=>{this.transitions?.next(Q(b({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Pt(null),this.transitions.pipe(Ie(i=>i!==null),pt(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return W(i).pipe(pt(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",cn.SupersededByNewNavigation),xt;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?Q(b({},l),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let u=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),m=c.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!u&&m!=="reload")return this.events.next(new kr(c.id,this.urlSerializer.serialize(c.rawUrl),"",El.IgnoredSameUrlNavigation)),c.resolve(!1),xt;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return W(c).pipe(pt(g=>(this.events.next(new _s(g.id,this.urlSerializer.serialize(g.extractedUrl),g.source,g.restoredState)),g.id!==this.navigationId?xt:Promise.resolve(g))),tL(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),tt(g=>{i.targetSnapshot=g.targetSnapshot,i.urlAfterRedirects=g.urlAfterRedirects,this.currentNavigation.update(_=>(_.finalUrl=g.urlAfterRedirects,_)),this.events.next(new Il)}),pt(g=>ot(i.routesRecognizeHandler.deferredHandle??W(void 0)).pipe(Y(()=>g))),tt(()=>{let g=new Sl(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(g)}));if(u&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:g,extractedUrl:_,source:C,restoredState:I,extras:j}=c,se=new _s(g,this.urlSerializer.serialize(_),C,I);this.events.next(se);let Ne=AE(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=Q(b({},c),{targetSnapshot:Ne,urlAfterRedirects:_,extras:Q(b({},j),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(ut=>(ut.finalUrl=_,ut)),W(i)}else return this.events.next(new kr(c.id,this.urlSerializer.serialize(c.extractedUrl),"",El.IgnoredByUrlHandlingStrategy)),c.resolve(!1),xt}),Y(c=>{let l=new nf(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=Q(b({},c),{guards:_F(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),kF(c=>this.events.next(c)),pt(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw pf(this.urlSerializer,c.guardsResult);let l=new rf(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!a())return xt;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",cn.GuardRejected),xt;if(c.guards.canActivateChecks.length===0)return W(c);let u=new of(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(u),!a())return xt;let m=!1;return W(c).pipe(nL(this.paramsInheritanceStrategy),tt({next:()=>{m=!0;let g=new sf(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(g)},complete:()=>{m||this.cancelNavigationTransition(c,"",cn.NoDataFromResolver)}}))}),mE(c=>{let l=m=>{let g=[];if(m.routeConfig?._loadedComponent)m.component=m.routeConfig?._loadedComponent;else if(m.routeConfig?.loadComponent){let _=m._environmentInjector;g.push(this.configLoader.loadComponent(_,m.routeConfig).then(C=>{m.component=C}))}for(let _ of m.children)g.push(...l(_));return g},u=l(c.targetSnapshot.root);return u.length===0?W(c):ot(Promise.all(u).then(()=>c))}),pt(c=>{let{newlyCreatedRoutes:l,state:u}=fF(e.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=Q(b({},c),{targetRouterState:u,newlyCreatedRoutes:l}),this.currentNavigation.update(m=>(m.targetRouterState=u,m)),W(c)}),this.activatedRouteInjectorFeature?.operator()??(c=>c),mE(()=>this.afterPreactivation()),pt(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,u=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return u?ot(u).pipe(Y(()=>i)):W(i)}),mt(1),pt(c=>{r=!1,this.events.next(new Sa);let l=i.beforeActivateHandler.deferredHandle;return l?ot(l.then(()=>c)):W(c)}),tt(c=>{new sb(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(l=>(l.abort=aL,l)),this.lastSuccessfulNavigation.set(Ce(this.currentNavigation)),this.events.next(new Di(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),pe(VE(s.signal).pipe(Ie(()=>!o&&r),tt(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",cn.Aborted)}))),tt({complete:()=>{o=!0}}),pe(this.transitionAbortWithErrorSubject.pipe(tt(c=>{throw c}))),Go(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",cn.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),zr(c=>{if(o=!0,fE(i),this.destroyed)return i.resolve(!1),xt;if(LE(c))this.events.next(new ri(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),gF(c)?this.events.next(new Ia(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new vs(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let u=zt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(u instanceof Ma){let{message:m,cancellationCode:g}=pf(this.urlSerializer,u);this.events.next(new ri(i.id,this.urlSerializer.serialize(i.extractedUrl),m,g)),this.events.next(new Ia(u.redirectTo,u.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(u){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(u)}}return xt}))}))}cancelNavigationTransition(e,i,r){fE(e);let o=new ri(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Ce(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function cL(t){return t!==Cl}function fE(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var eS=new y("");var tS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:()=>d(lL)})}return t})(),_f=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},lL=(()=>{class t extends _f{static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Cf=(()=>{class t{urlSerializer=d(Aa);options=d(Oa,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(oo);urlHandlingStrategy=d(yf);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new bn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof bn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=AE(null,d(Ue));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:()=>d(dL)})}return t})(),dL=(()=>{class t extends Cf{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof _s?this.updateStateMemento():e instanceof kr?this.commitTransition(i):e instanceof Sl?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Sa?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ri&&!TE(e)?this.restoreHistory(i):e instanceof vs?this.restoreHistory(i,!0):e instanceof Di&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=b(b({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=b(b({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?b({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):b({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function vb(t,n){t.events.pipe(Ie(e=>e instanceof Di||e instanceof ri||e instanceof vs||e instanceof kr),Y(e=>e instanceof Di||e instanceof kr?0:(e instanceof ri?e.code===cn.Redirect||e.code===cn.SupersededByNewNavigation:!1)?2:1),Ie(e=>e!==2),mt(1)).subscribe(()=>{n()})}var Ke=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(vm);stateManager=d(Cf);options=d(Oa,{optional:!0})||{};pendingTasks=d(hr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(JE);urlSerializer=d(Aa);location=d(oo);urlHandlingStrategy=d(yf);injector=d(Ue);_events=new S;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(tS);injectorCleanup=d(eS,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Pl,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(vf,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new he;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Ce(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof ri&&i.code!==cn.Redirect&&i.code!==cn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Di)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Ia){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=b({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||cL(r.source)},s);this.scheduleNavigation(a,Cl,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}dF(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Cl,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=Q(b({},o),{browserUrl:e})),r){let l=b({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Ln)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Ce(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(gb),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=b(b({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let m;try{let g=r?r.snapshot:this.routerState.snapshot.root;m=SE(g)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),m=this.currentUrlTree.root}return IE(m,e,u,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=co(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Cl,null,i)}navigate(e,i={skipLocationChange:!1}){return uL(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Pi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=b({},ub):i===!1?r=b({},xl):r=b(b({},xl),i),co(e))return Xv(this.currentUrlTree,e,r);let o=this.parseUrl(e);return Xv(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((m,g)=>{a=m,c=g});let u=this.pendingTasks.add();return vb(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function uL(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new k(4008,!1)}var fL=(()=>{class t{router=d(Ke);stateManager=d(Cf);fragment=V("");queryParams=V({});path=V("");serializer=d(Aa);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Di&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new bn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Nt=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new ti("href"),{optional:!0});reactiveHref=Sm(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Ce(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Ce(this._target)}_target=V(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Ce(this._queryParams)}_queryParams=V(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Ce(this._fragment)}_fragment=V(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Ce(this._queryParamsHandling)}_queryParamsHandling=V(void 0);set state(e){this._state.set(e)}get state(){return Ce(this._state)}_state=V(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Ce(this._info)}_info=V(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Ce(this._relativeTo)}_relativeTo=V(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Ce(this._preserveFragment)}_preserveFragment=V(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Ce(this._skipLocationChange)}_skipLocationChange=V(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Ce(this._replaceUrl)}_replaceUrl=V(!1);browserUrl=vn(void 0);isAnchorElement;onChanges=new S;applicationErrorHandler=d(Ln);options=d(Oa,{optional:!0});reactiveRouterState=d(fL);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=V(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(co(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=b({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(a,l)?.catch(u=>{this.applicationErrorHandler(u)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=dt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:co(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Ce(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(re(Ke),re(yn),Yc("tabindex"),re(we),re(F),re(ba))};static \u0275dir=T({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&A("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&G("href",r.reactiveHref(),R_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",L],skipLocationChange:[2,"skipLocationChange","skipLocationChange",L],replaceUrl:[2,"replaceUrl","replaceUrl",L],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[$e]})}return t})(),bb=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new U;link=d(Nt,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(s=>{s instanceof Di&&this.update()})}ngAfterContentInit(){W(this.links.changes,W(null)).pipe(Hr()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=ot(e).pipe(Hr()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=hL(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?b({},ub):b({},xl);return r=>{let o=r.urlTree;return o?Ce(mb(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(re(Ke),re(F),re(we),re(ge))};static \u0275dir=T({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,Nt,5),i&2){let s;z(s=$())&&(r.links=s)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[$e]})}return t})();function hL(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var pL=new y("");function yb(t,...n){return pi([{provide:Pl,multi:!0,useValue:t},{provide:yn,useFactory:gL},{provide:nl,multi:!0,useFactory:_L},n.map(e=>e.\u0275providers)])}function gL(){return d(Ke).routerState.root}function _L(){let t=d(q);return n=>{let e=t.get(Sn);if(n!==e.components[0])return;let i=t.get(Ke),r=t.get(vL);t.get(bL)===1&&i.initialNavigation(),t.get(yL,null,{optional:!0})?.setUpPreloading(),t.get(pL,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var vL=new y("",{factory:()=>new S}),bL=new y("",{factory:()=>1});var yL=new y("");var Cn={production:!1,apiUrl:"api/",hubUrl:"hub/notifications",stripePublicKey:"pk_test_51U51R4CZv3KqRJPpc5Qus4dVUgXpEH3oV76aQpLkHEDeNpBj1MLurjVuEIZN6QLLDehO2pbKS3ftSykA3pVztMT200UsKizsmf"};var lo=class t{baseUrl=Cn.apiUrl;httpClient=d(an);brandsList=[];typesList=[];getProducts(n){let e=new Bn;return n.brands&&n.brands.length>0&&(e=e.append("brands",n.brands.join(","))),n.types&&n.types.length>0&&(e=e.append("types",n.types.join(","))),n.sort&&(e=e.append("Sort",n.sort)),n.pageNumber&&(e=e.append("pageIndex",n.pageNumber.toString())),n.pageSize&&(e=e.append("pageSize",n.pageSize.toString())),n.search&&(e=e.append("search",n.search)),this.httpClient.get(this.baseUrl+"products",{params:e})}getProductById(n){return this.httpClient.get(this.baseUrl+"products/"+n)}getBrands(){if(!(this.brandsList.length>0))return this.httpClient.get(this.baseUrl+"products/brands").subscribe({next:n=>{this.brandsList=n},error:n=>{console.error("Error fetching brands:",n)}})}getTypes(){if(!(this.typesList.length>0))return this.httpClient.get(this.baseUrl+"products/types").subscribe({next:n=>{this.typesList=n},error:n=>{console.error("Error fetching types:",n)}})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};var CL=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(Z)}),wL=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function nS(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?wL.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Zt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=V("ltr");change=new U;constructor(){let e=d(CL,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(nS(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Wt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({})}return t})();var xL=["*"];var DL=new y("MAT_CARD_CONFIG"),uo=(()=>{class t{appearance;constructor(){let e=d(DL,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&H("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:xL,decls:1,vars:0,template:function(i,r){i&1&&(ke(),ie(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})();var iS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var rS=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&H("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})();var oS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Wt]})}return t})();function ys(t){return t.buttons===0||t.detail===0}function Cs(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Cb;function sS(){if(Cb==null){let t=typeof document<"u"?document.head:null;Cb=!!(t&&(t.createShadowRoot||t.attachShadow))}return Cb}function wb(t){if(sS()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Xi(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Qt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var xb;try{xb=typeof Intl<"u"&&Intl.v8BreakIterator}catch{xb=!1}var De=(()=>{class t{_platformId=d(ss);isBrowser=this._platformId?VD(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||xb)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Fl;function aS(){if(Fl==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Fl=!0}))}finally{Fl=Fl||!1}return Fl}function Pa(t){return aS()?t:!!t.capture}function Hn(t,n=0){return cS(t)?Number(t):arguments.length===2?n:0}function cS(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function wn(t){return t instanceof F?t.nativeElement:t}var lS=new y("cdk-input-modality-detector-options"),dS={ignoreKeys:[18,17,224,91,16]},uS=650,Db={passive:!0,capture:!0},mS=(()=>{class t{_platform=d(De);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Pt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Qt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<uS||(this._modality.next(ys(e)?"keyboard":"mouse"),this._mostRecentTarget=Qt(e))};_onTouchstart=e=>{if(Cs(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Qt(e)};constructor(){let e=d(B),i=d(Z),r=d(lS,{optional:!0});if(this._options=b(b({},dS),r),this.modalityDetected=this._modality.pipe(bc(1)),this.modalityChanged=this.modalityDetected.pipe(Qd()),this._platform.isBrowser){let o=d(At).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Db),o.listen(i,"mousedown",this._onMousedown,Db),o.listen(i,"touchstart",this._onTouchstart,Db)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Ll=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Ll||{}),fS=new y("cdk-focus-monitor-default-options"),wf=Pa({passive:!0,capture:!0}),An=(()=>{class t{_ngZone=d(B);_platform=d(De);_inputModalityDetector=d(mS);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(Z);_stopInputModalityDetector=new S;constructor(){let e=d(fS,{optional:!0});this._detectionMode=e?.detectionMode||Ll.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Qt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=wn(e);if(!this._platform.isBrowser||r.nodeType!==1)return W();let o=wb(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new S,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=wn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=wn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Ll.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Ll.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?uS:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Qt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,wf),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,wf)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(pe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,wf),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,wf),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var xf=new WeakMap,qe=(()=>{class t{_appRef;_injector=d(q);_environmentInjector=d(Ue);load(e){let i=this._appRef=this._appRef||this._injector.get(Sn),r=xf.get(i);r||(r={loaders:new Set,refs:[]},xf.set(i,r),i.onDestroy(()=>{xf.get(i)?.refs.forEach(o=>o.destroy()),xf.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Mm(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Ji=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Df;function SL(){if(Df===void 0&&(Df=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Df=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Df}function ws(t){return SL()?.createHTML(t)||t}function hS(t,n,e){let i=e.sanitize(Bt.HTML,n);t.innerHTML=ws(i||"")}function Fa(t){return Array.isArray(t)?t:[t]}var pS=new Set,xs,La=(()=>{class t{_platform=d(De);_nonce=d(Xr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):kL}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&IL(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function IL(t,n){if(!pS.has(t))try{xs||(xs=document.createElement("style"),n&&xs.setAttribute("nonce",n),xs.setAttribute("type","text/css"),document.head.appendChild(xs)),xs.sheet&&(xs.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),pS.add(t))}catch(e){console.error(e)}}function kL(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Eb=(()=>{class t{_mediaMatcher=d(La);_zone=d(B);_queries=new Map;_destroySubject=new S;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return gS(Fa(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=gS(Fa(e)).map(s=>this._registerQuery(s).observable),o=_c(r);return o=Ur(o.pipe(mt(1)),o.pipe(bc(1),$o(0))),o.pipe(Y(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new le(s=>{let a=c=>this._zone.run(()=>s.next(c));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(et(i),Y(({matches:s})=>({query:e,matches:s})),pe(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function gS(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function ML(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var TL=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),AL=(()=>{class t{_mutationObserverFactory=d(TL);_observedElements=new Map;_ngZone=d(B);ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=wn(e);return new le(r=>{let s=this._observeElement(i).pipe(Y(a=>a.filter(c=>!ML(c))),Ie(a=>!!a.length)).subscribe(a=>{this._ngZone.run(()=>{r.next(a)})});return()=>{s.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new S,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),_S=(()=>{class t{_contentObserver=d(AL);_elementRef=d(F);event=new U;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=Hn(e),this._subscribe()}_debounce;_currentSubscription=null;ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe($o(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",L],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})();var Ba=(()=>{class t{_platform=d(De);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return NL(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=RL(HL(e));if(i&&(vS(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=vS(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!VL(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return jL(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function RL(t){try{return t.frameElement}catch{return null}}function NL(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function OL(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function PL(t){return LL(t)&&t.type=="hidden"}function FL(t){return BL(t)&&t.hasAttribute("href")}function LL(t){return t.nodeName.toLowerCase()=="input"}function BL(t){return t.nodeName.toLowerCase()=="a"}function bS(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function vS(t){if(!bS(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function VL(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function jL(t){return PL(t)?!1:OL(t)||FL(t)||t.hasAttribute("contenteditable")||bS(t)}function HL(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Ef=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){vt(n,{injector:this._injector})}},Sb=(()=>{class t{_checker=d(Ba);_ngZone=d(B);_document=d(Z);_injector=d(q);constructor(){d(qe).load(Ji)}create(e,i=!1){return new Ef(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var yS=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),CS=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),UL=0,Bl=(()=>{class t{_ngZone=d(B);_defaultOptions=d(CS,{optional:!0});_liveElement;_document=d(Z);_sanitizer=d(_l);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(yS,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:hS(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${UL++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var zL=200,Sf=class{_letterKeyStream=new S;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new S;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:zL;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(tt(e=>this._pressedLetters.push(e)),$o(n),Ie(()=>this._pressedLetters.length>0),Y(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Ct(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Va=class{_items;_activeItemIndex=V(-1);_activeItem=V(null);_wrap=!1;_typeaheadSubscription=he.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof En?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Dn(n)&&(this._effectRef=Kn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new S;change=new S;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Sf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||Ct(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Dn(this._items)?this._items():this._items instanceof En?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Hl=class extends Va{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Mr=class extends Va{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var wS=new Map,Te=class t{_appId=d(pr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=wS.get(n);return i===void 0?i=0:i++,wS.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})};var ES=" ";function $L(t,n,e){let i=Mf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(ES)))}function GL(t,n,e){let i=Mf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(ES)):t.removeAttribute(n)}function Mf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var SS="cdk-describedby-message",kf="cdk-describedby-host",kb=0,Tf=(()=>{class t{_platform=d(De);_document=d(Z);_messageRegistry=new Map;_messagesContainer=null;_id=`${kb++}`;constructor(){d(qe).load(Ji),this._id=d(pr)+"-"+kb++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Ib(i,r);typeof i!="string"?(xS(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Ib(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${kf}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(kf);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");xS(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Ib(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=Mf(e,"aria-describedby").filter(r=>r.indexOf(SS)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);$L(e,"aria-describedby",r.messageElement.id),e.setAttribute(kf,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,GL(e,"aria-describedby",r.messageElement.id),e.removeAttribute(kf)}_isElementDescribedByMessage(e,i){let r=Mf(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function Ib(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function xS(t,n){t.id||(t.id=`${SS}-${n}-${kb++}`)}var Ss;function IS(){if(Ss==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Ss=!1,Ss;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Ss=!0;else{let t=Element.prototype.scrollTo;t?Ss=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Ss=!1}}return Ss}function Mb(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var ja,kS=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Tb(){if(ja)return ja;if(typeof document!="object"||!document)return ja=new Set(kS),ja;let t=document.createElement("input");return ja=new Set(kS.filter(n=>(t.setAttribute("type",n),t.type===n))),ja}var MS={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var WL=new y("MATERIAL_ANIMATIONS"),TS=null;function Ab(){return d(WL,{optional:!0})?.animationsDisabled||d(Oc,{optional:!0})==="NoopAnimations"?"di-disabled":(TS??=d(La).matchMedia("(prefers-reduced-motion)").matches,TS?"reduced-motion":"enabled")}function Ae(){return Ab()!=="enabled"}function It(t){return t==null?"":typeof t=="string"?t:`${t}px`}function kt(t){return t!=null&&`${t}`!="false"}var oi=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(oi||{}),Rb=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=oi.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},AS=Pa({passive:!0,capture:!0}),Nb=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,AS)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,AS)))}_delegateEventHandler=n=>{let e=Qt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Ul={enterDuration:225,exitDuration:150},qL=800,RS=Pa({passive:!0,capture:!0}),NS=["mousedown","touchstart"],OS=["mouseup","mouseleave","touchend","touchcancel"],YL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),Is=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Nb;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=wn(i)),o&&o.get(qe).load(YL)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=b(b({},Ul),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||ZL(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${c-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(u);let m=window.getComputedStyle(u),g=m.transitionProperty,_=m.transitionDuration,C=g==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,I=new Rb(this,u,i,C);u.style.transform="scale3d(1, 1, 1)",I.state=oi.FADING_IN,i.persistent||(this._mostRecentTransientRipple=I);let j=null;return!C&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let se=()=>{j&&(j.fallbackTimer=null),clearTimeout(ut),this._finishRippleTransition(I)},Ne=()=>this._destroyRipple(I),ut=setTimeout(Ne,l+100);u.addEventListener("transitionend",se),u.addEventListener("transitioncancel",Ne),j={onTransitionEnd:se,onTransitionCancel:Ne,fallbackTimer:ut}}),this._activeRipples.set(I,j),(C||!l)&&this._finishRippleTransition(I),I}fadeOutRipple(n){if(n.state===oi.FADING_OUT||n.state===oi.HIDDEN)return;let e=n.element,i=b(b({},Ul),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=oi.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=wn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,NS.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{OS.forEach(e=>{this._triggerElement.addEventListener(e,this,RS)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===oi.FADING_IN?this._startFadeOutTransition(n):n.state===oi.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=oi.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=oi.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ys(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+qL;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Cs(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===oi.VISIBLE||n.config.terminateOnPointerUp&&n.state===oi.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(NS.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(OS.forEach(e=>n.removeEventListener(e,this,RS)),this._pointerUpEventsRegistered=!1))}};function ZL(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var zl=new y("mat-ripple-global-options"),er=(()=>{class t{_elementRef=d(F);_animationsDisabled=Ae();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(B),i=d(De),r=d(zl,{optional:!0}),o=d(q);this._globalOptions=r||{},this._rippleRenderer=new Is(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:b(b(b({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,b(b({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,b(b({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&H("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var QL={capture:!0},KL=["focus","mousedown","mouseenter","touchstart"],Ob="mat-ripple-loader-uninitialized",Pb="mat-ripple-loader-class-name",PS="mat-ripple-loader-centered",Af="mat-ripple-loader-disabled",FS=(()=>{class t{_document=d(Z);_animationsDisabled=Ae();_globalRippleOptions=d(zl,{optional:!0});_platform=d(De);_ngZone=d(B);_injector=d(q);_eventCleanups;_hosts=new Map;constructor(){let e=d(At).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>KL.map(i=>e.listen(this._document,i,this._onInteraction,QL)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Ob,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Pb))&&e.setAttribute(Pb,i.className||""),i.centered&&e.setAttribute(PS,""),i.disabled&&e.setAttribute(Af,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Af,""):e.removeAttribute(Af)}_onInteraction=e=>{let i=Qt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Ob}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Pb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ul.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ul.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Af),rippleConfig:{centered:e.hasAttribute(PS),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new Is(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(Ob)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Rn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
    --mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var XL=["*",[["","progressIndicator",""]]],JL=["*","[progressIndicator]"];function e2(t,n){t&1&&(Ve(0,"div",1),ie(1,1),Qe())}var t2=new y("MAT_BUTTON_CONFIG");function LS(t){return t==null?void 0:Et(t)}var Fb=(()=>{class t{_elementRef=d(F);_ngZone=d(B);_animationsDisabled=Ae();_config=d(t2,{optional:!0});_focusMonitor=d(An);_cleanupClick;_renderer=d(we);_rippleLoader=d(FS);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=vn(!1,{transform:L});constructor(){d(qe).load(Rn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(G("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),jt(r.color?"mat-"+r.color:""),H("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",L],disabled:[2,"disabled","disabled",L],ariaDisabled:[2,"aria-disabled","ariaDisabled",L],disabledInteractive:[2,"disabledInteractive","disabledInteractive",L],tabIndex:[2,"tabIndex","tabIndex",LS],_tabindex:[2,"tabindex","_tabindex",LS],showProgress:[1,"showProgress"]}})}return t})(),Lb=(()=>{class t extends Fb{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[be],ngContentSelectors:JL,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(ke(XL),In(0,"span",0),ie(1),O(2,e2,2,0,"div",1),In(3,"span",2)(4,"span",3)),i&2&&(p(2),P(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var Ha=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Wt]})}return t})();var n2=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],i2=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function r2(t,n){t&1&&(Ve(0,"div",2),ie(1,3),Qe())}var BS=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),it=(()=>{class t extends Fb{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=o2(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?BS.get(this._appearance):null,o=BS.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[be],ngContentSelectors:i2,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(ke(n2),In(0,"span",0),ie(1),Ve(2,"span",1),ie(3,1),Qe(),ie(4,2),O(5,r2,2,0,"div",2),In(6,"span",3)(7,"span",4)),i&2&&(H("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),p(5),P(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --mat-progress-spinner-active-indicator-color: var(--mat-button-filled-progress-active-indicator-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function o2(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Bb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Ha,Wt]})}return t})();function VS(t){return Error(`Unable to find icon with the name "${t}"`)}function a2(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function jS(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function HS(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Tr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},zS=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Tr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(Bt.HTML,r);if(!s)throw HS(r);let a=ws(s);return this._addSvgIconConfig(e,i,new Tr("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Tr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Bt.HTML,i);if(!o)throw HS(i);let s=ws(o);return this._addSvgIconSetConfig(e,new Tr("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Bt.RESOURCE_URL,e);if(!i)throw jS(e);let r=this._cachedIconsByUrl.get(i);return r?W(Rf(r)):this._loadSvgIconFromConfig(new Tr(e,null)).pipe(tt(o=>this._cachedIconsByUrl.set(i,o)),Y(o=>Rf(o)))}getNamedSvgIcon(e,i=""){let r=US(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):pc(VS(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?W(Rf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(Y(i=>Rf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return W(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(zr(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(Bt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),W(null)})));return Uo(o).pipe(Y(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw VS(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(tt(i=>e.svgText=i),Y(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?W(null):this._fetchIcon(e).pipe(tt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(ws("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(ws("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw a2();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(Bt.RESOURCE_URL,i);if(!s)throw jS(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(Y(l=>ws(l)),Go(()=>this._inProgressUrlFetches.delete(s)),vc());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(US(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return c2(o)?new Tr(o.url,null,o.options):new Tr(o,null)}}static \u0275fac=function(i){return new(i||t)(ne(an,8),ne(_l),ne(Z,8),ne(Jt))};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Rf(t){return t.cloneNode(!0)}function US(t,n){return t+":"+n}function c2(t){return!!(t.url&&t.options)}var l2=["*"],d2=new y("MAT_ICON_DEFAULT_OPTIONS"),u2=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(Z),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),$S=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],m2=$S.map(t=>`[${t}]`).join(", "),f2=/^url\(['"]?#(.*?)['"]?\)$/,ln=(()=>{class t{_elementRef=d(F);_iconRegistry=d(zS);_location=d(u2);_errorHandler=d(Jt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=he.EMPTY;constructor(){let e=d(new ti("aria-hidden"),{optional:!0}),i=d(d2,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(m2),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)$S.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(f2):null;if(l){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(mt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(G("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),jt(r.color?"mat-"+r.color:""),H("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",L],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:l2,decls:1,vars:0,template:function(i,r){i&1&&(ke(),ie(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})(),Nf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Wt]})}return t})();var GS="useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict";var WS=(t=21)=>{let n="",e=crypto.getRandomValues(new Uint8Array(t|=0));for(;t--;)n+=GS[e[t]&63];return n};var Of=class{id=WS();cartItems=[];deliveryMethodId;paymentIntentId;clientSecret};var rt=class t{baseUrl=Cn.apiUrl;http=d(an);cart=V(null);selectedDelivery=V(null);itemCount=dt(()=>this.cart()?.cartItems.reduce((n,e)=>n+e.quantity,0));totals=dt(()=>{let n=this.cart(),e=this.selectedDelivery();if(!n)return null;let i=n.cartItems.reduce((a,c)=>a+c.price*c.quantity,0),r=0,o=e?e.price:0,s=i+o-r;return{subtotal:i,shipping:o,discount:r,total:s}});getCart(n){return this.http.get(this.baseUrl+"cart?id="+n).pipe(Y(e=>(this.cart.set(e),e)))}setCart(n){return this.http.post(this.baseUrl+"cart",n).subscribe({next:e=>this.cart.set(e)})}deleteCart(){this.http.delete(this.baseUrl+"cart?id="+this.cart()?.id).subscribe({next:()=>{localStorage.removeItem("cart_id"),this.cart.set(null)}})}addItemsToCart(n,e=1){let i=this.cart()??this.CreateCart();this.isProduct(n)&&(n=this.mapProductToCart(n)),i.cartItems=this.addOrUpdateCartItem(i.cartItems,n,e),this.setCart(i)}removeItemFromCart(n,e=1){let i=this.cart();if(!i)return;let r=i.cartItems.findIndex(o=>o.productId===n);r!==-1&&(i.cartItems[r].quantity>e?i.cartItems[r].quantity-=e:i.cartItems.splice(r,1),i.cartItems.length===0?this.deleteCart():this.setCart(i))}addOrUpdateCartItem(n,e,i){if(n.length>0){let r=n.findIndex(o=>o.productId==e.productId);r==-1?(e.quantity=i,n.push(e)):n[r].quantity+=i}else e.quantity=i,n.push(e);return n}mapProductToCart(n){return{productId:n.id,productName:n.name,price:n.price,quantity:0,pictureUrl:n.pictureUrl,brand:n.brand,type:n.type}}isProduct(n){return n.id!==void 0}CreateCart(){let n=new Of;return localStorage.setItem("cart_id",n.id),n}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};function p2(t,n){if(t&1){let e=lt();f(0,"mat-card",0),M(1,"img",1),f(2,"mat-card-content")(3,"h2",2),v(4),h(),f(5,"p",3),v(6),xe(7,"currency"),h()(),f(8,"mat-card-actions")(9,"button",4),A("click",function(){Ee(e);let r=E();return Se(r.addItemToCartService(r.product))}),f(10,"mat-icon"),v(11,"add_shopping_cart"),h(),v(12," Add to Cart "),h()()()}if(t&2){let e=E();p(),x("src",kn(e.product.pictureUrl),Xn)("routerLink",Jn("/shop/",e.product.id))("alt",kn(e.product.name)),p(3),X(e.product.name),p(2),X(Me(7,8,e.product.price))}}var Pf=class t{product;cartService=d(rt);addItemToCartService(n){this.cartService.addItemsToCart(n)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-product-item"]],inputs:{product:"product"},decls:1,vars:1,consts:[["appearance","raised"],["mat-card-image","",1,"product-card",2,"object-fit","cover","width","100%","height","300px",3,"src","routerLink","alt"],[1,"text-sm","font-semibold","uppercase"],[1,"font-light"],["mat-raised-button","","color","primary",1,"w-full",3,"click"]],template:function(e,i){e&1&&O(0,p2,13,10,"mat-card",0),e&2&&P(i.product?0:-1)},dependencies:[uo,iS,rS,Nf,ln,Bb,it,Nt,St],styles:[".product-card[_ngcontent-%COMP%]{transition:transform .2s,box-shadow .2s}.product-card[_ngcontent-%COMP%]:hover{transform:translateY(-10px);box-shadow:0 4px 8px #0003;cursor:pointer}.mat-mdc-raised-button[_ngcontent-%COMP%]:hover{box-shadow:0 0 10px #1976d280}"]})};var g2=20,Ua=(()=>{class t{_ngZone=d(B);_platform=d(De);_renderer=d(At).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new S;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=g2){return this._platform.isBrowser?new le(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Zd(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):W()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ie(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=wn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var _2=20,mo=(()=>{class t{_platform=d(De);_listeners;_viewportSize=null;_change=new S;_document=d(Z);constructor(){let e=d(B),i=d(At).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=_2){return e>0?this._change.pipe(Zd(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var $l=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Ei=class extends $l{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Un=class extends $l{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Vb=class extends $l{element;constructor(n){super(),this.element=n instanceof F?n.nativeElement:n}},fo=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Ei)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Un)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Vb)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Gl=class extends fo{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(qi,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||q.NULL,o=r.get(Ue,i.injector);e=Mm(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var tr=(()=>{class t extends fo{_moduleRef=d(qi,{optional:!0});_document=d(Z);_viewContainerRef=d(Vt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new U;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[be]})}return t})(),za=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({})}return t})();var qS=IS();function Wl(t){return new Ff(t.get(mo),t.get(Z))}var Ff=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=It(-this._previousScrollPosition.left),n.style.top=It(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),qS&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),qS&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};var Lf=class{enable(){}disable(){}attach(){}};function jb(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function YS(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function ho(t,n){return new Bf(t.get(Ua),t.get(mo),t.get(B),n)}var Bf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();jb(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}};var Si=class{positionStrategy;scrollStrategy=new Lf;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Vf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var eI=(()=>{class t{_attachedOverlays=[];_document=d(Z);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),tI=(()=>{class t extends eI{_ngZone=d(B);_renderer=d(At).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),nI=(()=>{class t extends eI{_platform=d(De);_ngZone=d(B);_renderer=d(At).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Qt(e)};_clickListener=e=>{let i=Qt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(ZS(a.overlayElement,i)||ZS(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function ZS(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var iI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),Uf=(()=>{class t{_platform=d(De);_containerElement;_document=d(Z);_styleLoader=d(qe);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Mb()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Mb()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(iI)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Hb=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Ub(t){return t&&t.nodeType===1}var $a=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new S;_attachments=new S;_detachments=new S;_positionStrategy;_scrollStrategy;_locationChanges=he.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new S;_outsidePointerEvents=new S;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,u=!1,m,g){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=u,this._injector=m,this._renderer=g,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=vt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=b(b({},this._config),n),this._updateElementSize()}setDirection(n){this._config=Q(b({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=It(this._config.width),n.height=It(this._config.height),n.minWidth=It(this._config.minWidth),n.minHeight=It(this._config.minHeight),n.maxWidth=It(this._config.maxWidth),n.maxHeight=It(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Ub(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Hb(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Fa(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=vt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},QS="cdk-overlay-connected-position-bounding-box",v2=/([A-Za-z%]+)$/;function Wa(t,n){return new jf(n,t.get(mo),t.get(Z),t.get(De),t.get(Uf))}var jf=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new S;_resizeSubscription=he.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(QS),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),u=this._getOverlayFit(l,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(u,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let u=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);u>c&&(c=u,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&ks(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(QS),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof F?this._origin.nativeElement:Ub(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=XS(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let u=0-s,m=s+o.width-i.width,g=0-a,_=a+o.height-i.height,C=this._subtractOverflows(o.width,u,m),I=this._subtractOverflows(o.height,g,_),j=C*I;return{visibleArea:j,isCompletelyWithinViewport:o.width*o.height===j,fitsInViewportVertically:I===o.height,fitsInViewportHorizontally:C==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=KS(this._overlayRef.getConfig().minHeight),a=KS(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=XS(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),u=0,m=0;return r.width<=o.width?u=l||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?m=c||-a:m=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:m},{x:n.x+u,y:n.y+m}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!b2(this._lastScrollVisibility,i)){let r=new Vf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),C=this._lastBoundingBoxSize.height;o=_*2,s=n.y-_,o>C&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-C/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,m,g;if(l)g=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(c)m=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),C=this._lastBoundingBoxSize.width;u=_*2,m=n.x-_,u>C&&!this._isInitialRender&&!this._growAfterOpen&&(m=n.x-C/2)}return{top:s,left:m,bottom:a,right:g,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=It(i.width),r.height=It(i.height),r.top=It(i.top)||"auto",r.bottom=It(i.bottom)||"auto",r.left=It(i.left)||"auto",r.right=It(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=It(o)),s&&(r.maxWidth=It(s))}this._lastBoundingBoxSize=i,ks(this._boundingBox.style,r)}_resetBoundingBoxStyles(){ks(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){ks(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();ks(i,this._getExactOverlayY(e,n,u)),ks(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=It(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=It(s.maxWidth):o&&(i.maxWidth="")),ks(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=It(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=It(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:YS(n,i),isOriginOutsideView:jb(n,i),isOverlayClipped:YS(e,i),isOverlayOutsideView:jb(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Fa(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof F)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function ks(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function KS(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(v2);return!e||e==="px"?parseFloat(n):null}return t||null}function XS(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function b2(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var JS="cdk-global-overlay-wrapper";function Ms(t){return new Hf}var Hf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(JS),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,m=this._xOffset,g=this._overlayRef.getConfig().direction==="rtl",_="",C="",I="";c?I="flex-start":u==="center"?(I="center",g?C=m:_=m):g?u==="left"||u==="end"?(I="flex-end",_=m):(u==="right"||u==="start")&&(I="flex-start",C=m):u==="left"||u==="start"?(I="flex-start",_=m):(u==="right"||u==="end")&&(I="flex-end",C=m),n.position=this._cssPosition,n.marginLeft=c?"0":_,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":C,e.justifyContent=I,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(JS),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}};var ql=new y("OVERLAY_DEFAULT_CONFIG");function nr(t,n){t.get(qe).load(iI);let e=t.get(Uf),i=t.get(Z),r=t.get(Te),o=t.get(Sn),s=t.get(Zt),a=t.get(we,null,{optional:!0})||t.get(At).createRenderer(null,null),c=new Si(n),l=t.get(ql,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,!i.body||!("showPopover"in i.body)?c.usePopover=!1:c.usePopover=n?.usePopover??l;let u=i.createElement("div"),m=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),m.appendChild(u),c.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let g=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Ub(g)?g.after(m):g?.type==="parent"?g.element.appendChild(m):e.getContainerElement().appendChild(m),new $a(new Gl(u,o,t),m,u,c,t.get(B),t.get(tI),i,t.get(oo),t.get(nI),n?.disableAnimations??t.get(Oc,null,{optional:!0})==="NoopAnimations",t.get(Ue),a)}var y2=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],C2=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(q);return()=>ho(t)}}),Ga=(()=>{class t{elementRef=d(F);static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),rI=new y("cdk-connected-overlay-default-config"),zf=(()=>{class t{_dir=d(Zt,{optional:!0});_injector=d(q);_overlayRef;_templatePortal;_backdropSubscription=he.EMPTY;_attachSubscription=he.EMPTY;_detachSubscription=he.EMPTY;_positionSubscription=he.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(C2);_ngZone=d(B);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new U;positionChange=new U;attach=new U;detach=new U;overlayKeydown=new U;overlayOutsideClick=new U;constructor(){let e=d(gt),i=d(Vt),r=d(rI,{optional:!0}),o=d(ql,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Un(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=y2);let e=this._overlayRef=nr(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ct(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Qt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Si({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Wa(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ga?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ga?this.origin.elementRef.nativeElement:this.origin instanceof F?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(kp(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",L],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",L],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",L],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",L],push:[2,"cdkConnectedOverlayPush","push",L],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",L],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",L],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[$e]})}return t})();function x2(t,n){}var po=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var $b=(()=>{class t extends fo{_elementRef=d(F);_focusTrapFactory=d(Sb);_config;_interactivityChecker=d(Ba);_ngZone=d(B);_focusMonitor=d(An);_renderer=d(we);_changeDetectorRef=d(ge);_injector=d(q);_platform=d(De);_document=d(Z);_portalOutlet;_focusTrapped=new S;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(po,{optional:!0})||new po,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||vt(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Xi(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Xi();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Xi()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&je(tr,7),i&2){let o;z(o=$())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&G("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[be],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Ge(0,x2,0,0,"ng-template",0)},dependencies:[tr],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})}return t})(),Zl=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new S;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Ct(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},D2=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(q);return()=>Wl(t)}}),E2=new y("DialogData"),S2=new y("DefaultDialogConfig");function I2(t){let n=V(t),e=new U;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var oI=(()=>{class t{_injector=d(q);_defaultOptions=d(S2,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Uf);_idGenerator=d(Te);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new S;_afterOpenedAtThisLevel=new S;_ariaHiddenElements=new Map;_scrollStrategy=d(D2);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=hi(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(et(void 0)));open(e,i){let r=this._defaultOptions||new po;i=b(b({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=nr(this._injector,o),a=new Zl(s,i),c=this._attachContainer(s,a,i);if(a.containerInstance=c,!this.openDialogs.length){let l=this._overlayContainer.getContainerElement();c._focusTrapped?c._focusTrapped.pipe(mt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(l)}):this._hideNonDialogContentFromAssistiveTechnology(l)}return this._attachDialogContent(e,a,c,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){zb(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){zb(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),zb(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Si({positionStrategy:e.positionStrategy||Ms().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:po,useValue:r},{provide:Zl,useValue:i},{provide:$a,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=$b;let c=new Ei(a,r.viewContainerRef,q.create({parent:o||this._injector,providers:s}));return e.attach(c).instance}_attachDialogContent(e,i,r,o){if(e instanceof gt){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=b(b({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Un(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new Ei(e,o.viewContainerRef,s,null,o.bindings));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:E2,useValue:e.data},{provide:Zl,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(Zt,null,{optional:!0}))&&a.push({provide:Zt,useValue:I2(e.direction)}),q.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function zb(t,n){let e=t.length;for(;e--;)n(t[e])}function k2(t,n){}var Gf=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},Gb="mdc-dialog--open",sI="mdc-dialog--opening",aI="mdc-dialog--closing",M2=150,T2=75,A2=(()=>{class t extends $b{_animationStateChanged=new U;_animationsEnabled=!Ae();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?lI(this._config.enterAnimationDuration)??M2:0;_exitAnimationDuration=this._animationsEnabled?lI(this._config.exitAnimationDuration)??T2:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(cI,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(sI,Gb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Gb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Gb),this._animationsEnabled?(this._hostElement.style.setProperty(cI,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(aI)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(sI,aI)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275cmp=D({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(ht("id",r._config.id),G("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),H("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[be],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",0)(1,"div",1),Ge(2,k2,0,0,"ng-template",2),h()())},dependencies:[tr],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2,changeDetection:1})}return t})(),cI="--mat-dialog-transition-duration";function lI(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Hn(t.substring(0,t.length-2)):t.endsWith("s")?Hn(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var $f=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})($f||{}),Ql=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Ri(1);_beforeClosed=new Ri(1);_result;_closeFallbackTimeout;_state=$f.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(Ie(r=>r.state==="opened"),mt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ie(r=>r.state==="closed"),mt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),fn(this.backdropClick(),this.keydownEvents().pipe(Ie(r=>r.keyCode===27&&!this.disableClose&&!Ct(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),R2(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(Ie(i=>i.state==="closing"),mt(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=$f.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=$f.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function R2(t,n,e){return t._closeInteractionType=n,t.close(e)}var Wb=new y("MatMdcDialogData"),N2=new y("mat-mdc-dialog-default-options"),O2=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(q);return()=>Wl(t)}}),dI=(()=>{class t{_defaultOptions=d(N2,{optional:!0});_scrollStrategy=d(O2);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Te);_injector=d(q);_dialog=d(oI);_animationsDisabled=Ae();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new S;_afterOpenedAtThisLevel=new S;dialogConfigClass=Gf;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=hi(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(et(void 0)));constructor(){this._dialogRefConstructor=Ql,this._dialogContainerType=A2,this._dialogDataToken=Wb}open(e,i){let r;i=b(b({},this._defaultOptions||new Gf),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,Q(b({},i),{positionStrategy:Ms(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:po,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,c)=>(r=new this._dialogRefConstructor(s,i,c),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:c},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var go=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new S;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var qb=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var bI=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(re(we),re(F))};static \u0275dir=T({type:t})}return t})(),yI=(()=>{class t extends bI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,features:[be]})}return t})(),ir=new y("");var P2={provide:ir,useExisting:Mt(()=>rr),multi:!0};function F2(){let t=ni()?ni().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var L2=new y(""),rr=(()=>{class t extends bI{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!F2())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(re(we),re(F),re(L2,8))};static \u0275dir=T({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&A("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Oe([P2]),be]})}return t})();function Jb(t){return t==null||ey(t)===0}function ey(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var vo=new y(""),th=new y(""),B2=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,zn=class{static min(n){return CI(n)}static max(n){return V2(n)}static required(n){return wI(n)}static requiredTrue(n){return j2(n)}static email(n){return H2(n)}static minLength(n){return U2(n)}static maxLength(n){return z2(n)}static pattern(n){return $2(n)}static nullValidator(n){return qf()}static compose(n){return kI(n)}static composeAsync(n){return MI(n)}};function CI(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function V2(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function wI(t){return Jb(t.value)?{required:!0}:null}function j2(t){return t.value===!0?null:{required:!0}}function H2(t){return Jb(t.value)||B2.test(t.value)?null:{email:!0}}function U2(t){return n=>{let e=n.value?.length??ey(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function z2(t){return n=>{let e=n.value?.length??ey(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function $2(t){if(!t)return qf;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Jb(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function qf(t){return null}function xI(t){return t!=null}function DI(t){return yr(t)?ot(t):t}function EI(t){let n={};return t.forEach(e=>{n=e!=null?b(b({},n),e):n}),Object.keys(n).length===0?null:n}function SI(t,n){return n.map(e=>e(t))}function G2(t){return!t.validate}function II(t){return t.map(n=>G2(n)?n:e=>n.validate(e))}function kI(t){if(!t)return null;let n=t.filter(xI);return n.length==0?null:function(e){return EI(SI(e,n))}}function ty(t){return t!=null?kI(II(t)):null}function MI(t){if(!t)return null;let n=t.filter(xI);return n.length==0?null:function(e){let i=SI(e,n).map(DI);return Uo(i).pipe(Y(EI))}}function ny(t){return t!=null?MI(II(t)):null}function uI(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function TI(t){return t._rawValidators}function AI(t){return t._rawAsyncValidators}function Yb(t){return t?Array.isArray(t)?t:[t]:[]}function Yf(t,n){return Array.isArray(t)?t.includes(n):t===n}function mI(t,n){let e=Yb(n);return Yb(t).forEach(r=>{Yf(e,r)||e.push(r)}),e}function fI(t,n){return Yb(n).filter(e=>!Yf(t,e))}var Zf=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=ty(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=ny(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Ii=class extends Zf{name;get formDirective(){return null}get path(){return null}};var Kl="VALID",Wf="INVALID",qa="PENDING",Xl="DISABLED",_o=class{},Qf=class extends _o{value;source;constructor(n,e){super(),this.value=n,this.source=e}},ed=class extends _o{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},td=class extends _o{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Ya=class extends _o{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Kf=class extends _o{source;constructor(n){super(),this.source=n}},Ts=class extends _o{source;constructor(n){super(),this.source=n}};function iy(t){return(nh(t)?t.validators:t)||null}function W2(t){return Array.isArray(t)?ty(t):t||null}function ry(t,n){return(nh(n)?n.asyncValidators:t)||null}function q2(t){return Array.isArray(t)?ny(t):t||null}function nh(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function RI(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new k(1e3,"");if(!OI(i,e))throw new k(1001,"")}function NI(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new k(-1002,"")})}var Za=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=V(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Ce(this.statusReactive)}set status(n){Ce(()=>this.statusReactive.set(n))}_status=dt(()=>this.statusReactive());statusReactive=V(void 0);get valid(){return this.status===Kl}get invalid(){return this.status===Wf}get pending(){return this.status===qa}get disabled(){return this.status===Xl}get enabled(){return this.status!==Xl}errors;get pristine(){return Ce(this.pristineReactive)}set pristine(n){Ce(()=>this.pristineReactive.set(n))}_pristine=dt(()=>this.pristineReactive());pristineReactive=V(!0);get dirty(){return!this.pristine}get touched(){return Ce(this.touchedReactive)}set touched(n){Ce(()=>this.touchedReactive.set(n))}_touched=dt(()=>this.touchedReactive());touchedReactive=V(!1);get untouched(){return!this.touched}_events=new S;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(mI(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(mI(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(fI(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(fI(n,this._rawAsyncValidators))}hasValidator(n){return Yf(this._rawValidators,n)}hasAsyncValidator(n){return Yf(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(Q(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new td(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new td(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(Q(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ed(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new ed(!0,i))}markAsPending(n={}){this.status=qa;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Ya(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(Q(b({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Xl,this.errors=null,this._forEachChild(r=>{r.disable(Q(b({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Qf(this.value,i)),this._events.next(new Ya(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(Q(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Kl,this._forEachChild(i=>{i.enable(Q(b({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(Q(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Kl||this.status===qa)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Qf(this.value,e)),this._events.next(new Ya(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(Q(b({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Xl:Kl}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=qa,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=DI(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Ya(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new U,this.statusChanges=new U}_calculateStatus(){return this._allControlsDisabled()?Xl:this.errors?Wf:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(qa)?qa:this._anyControlsHaveStatus(Wf)?Wf:Kl}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new ed(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new td(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){nh(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=W2(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=q2(this._rawAsyncValidators)}_updateHasRequiredValidator(){Ce(()=>this._hasRequired.set(this.hasValidator(zn.required)))}};function OI(t,n){return Object.hasOwn(t,n)}function Y2(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function Z2(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Zb=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};function Q2(t){return typeof t=="number"?t:parseFloat(t)}var PI=(()=>{class t{_validator=qf;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):qf,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,features:[$e]})}return t})();var K2={provide:vo,useExisting:Mt(()=>oy),multi:!0},oy=(()=>{class t extends PI{min;inputName="min";normalizeInput=e=>Q2(e);createValidator=e=>CI(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["input","type","number","min","","formControlName",""],["input","type","number","min","","formControl",""],["input","type","number","min","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&G("min",r._enabled?r.min:null)},inputs:{min:"min"},standalone:!1,features:[Oe([K2]),be]})}return t})(),X2={provide:vo,useExisting:Mt(()=>FI),multi:!0};var FI=(()=>{class t extends PI{required;inputName="required";normalizeInput=L;createValidator=e=>wI;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&G("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Oe([X2]),be]})}return t})();var J2=new y(""),id=new y("",{factory:()=>ih}),ih="always";function LI(t,n){return[...n.path,t]}function Qb(t,n,e=ih){sy(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),tB(t,n),iB(t,n),nB(t,n),eB(t,n)}function hI(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Jf(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Xf(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function eB(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function sy(t,n){let e=TI(t);n.validator!==null?t.setValidators(uI(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=AI(t);n.asyncValidator!==null?t.setAsyncValidators(uI(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Xf(n._rawValidators,r),Xf(n._rawAsyncValidators,r)}function Jf(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=TI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=AI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Xf(n._rawValidators,i),Xf(n._rawAsyncValidators,i),e}function tB(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&BI(t,n)})}function nB(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&BI(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function BI(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function iB(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function VI(t,n){t==null,sy(t,n)}function rB(t,n){return Jf(t,n)}function jI(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function oB(t){return Object.getPrototypeOf(t.constructor)===yI}function HI(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function sB(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===rr?e=o:oB(o)?i=o:r=o}),r||i||e||null}function aB(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var UI={provide:J2,useFactory:()=>{let t=d(si,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},si=class extends Zf{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Ts&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=sB(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(Lt)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(ge);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new he,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Ts&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=Y2(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof FI))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&Z2(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new Zb({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=dt(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Kn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},eh=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var or=(()=>{class t extends eh{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(re(si,2))};static \u0275dir=T({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&H("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[be]})}return t})(),bo=(()=>{class t extends eh{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(re(Ii,10))};static \u0275dir=T({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&H("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[be]})}return t})(),Qa=class extends Za{constructor(n,e,i){super(iy(e),ry(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Ce(()=>{NI(this,!0,n),Object.keys(n).forEach(i=>{RI(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,Q(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Ts(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return OI(this.controls,n)?this.controls[n]:null}};var Kb=class extends Qa{};var cB={provide:Ii,useExisting:Mt(()=>Rr)},Jl=Promise.resolve(),Rr=(()=>{class t extends Ii{callSetDisabledState;get submitted(){return Ce(this.submittedReactive)}_submitted=dt(()=>this.submittedReactive());submittedReactive=V(!1);_directives=new Set;form;ngSubmit=new U;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Qa({},ty(e),ny(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Jl.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Jl.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Jl.then(()=>{let i=this._findContainer(e.path),r=new Qa({});VI(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Jl.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Jl.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),HI(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Kf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(re(vo,10),re(th,10),re(id,8))};static \u0275dir=T({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&A("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Oe([cB]),be]})}return t})();function pI(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function gI(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var nd=class extends Za{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(iy(e),ry(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),nh(e)&&(e.nonNullable||e.initialValueIsDefault)&&(gI(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Ce(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Ts(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){pI(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){pI(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){gI(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var lB=t=>t instanceof nd;var dB={provide:si,useExisting:Mt(()=>As)},_I=Promise.resolve(),As=(()=>{class t extends si{_changeDetectorRef;callSetDisabledState;control=new nd;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new U;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),jI(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,Qb(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,Qb(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){_I.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&L(i);_I.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?LI(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(re(Ii,9),re(vo,10),re(th,10),re(ir,10),re(ge,8),re(id,8),re(q,8),re(we,8))};static \u0275dir=T({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Oe([dB,UI]),be,$e,_m(null)]})}return t})();var yo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})(),uB={provide:ir,useExisting:Mt(()=>ay),multi:!0},ay=(()=>{class t extends yI{writeValue(e){let i=e??"";this.setProperty("value",i)}registerOnChange(e){this.onChange=i=>{e(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["input","type","number","formControlName","",3,"ngNoCva",""],["input","type","number","formControl","",3,"ngNoCva",""],["input","type","number","ngModel","",3,"ngNoCva",""]],hostBindings:function(i,r){i&1&&A("input",function(s){return r.onChange(s.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Oe([uB]),be]})}return t})();var Xb=class extends Za{constructor(n,e,i){super(iy(e),ry(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){Ce(()=>{NI(this,!1,n),n.forEach((i,r)=>{RI(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],Q(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Ts(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var mB=(()=>{class t extends Ii{callSetDisabledState;get submitted(){return Ce(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=dt(()=>this._submittedReactive());_submittedReactive=V(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Jf(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){hI(e.control||null,e,!1),aB(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,HI(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Kf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(hI(i||null,e),lB(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);VI(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&rB(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){sy(this.form,this),this._oldForm&&Jf(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(re(vo,10),re(th,10),re(id,8))};static \u0275dir=T({type:t,features:[be,$e]})}return t})();var zI=new y("");var fB={provide:si,useExisting:Mt(()=>rd)},rd=(()=>{class t extends si{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new U;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s,a,c){super(c,a,o),this._ngModelWarningConfig=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}_setupWithForm(e,i){this.control=e,this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,Qb(e,this,i))}ngOnChanges(e){this._added||this._setUpControl(),jI(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return LI(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){this.isCustomControlBased&&(this._added||this._setUpControl(),super.ngControlUpdate(e,!0))}static \u0275fac=function(i){return new(i||t)(re(Ii,13),re(vo,10),re(th,10),re(ir,10),re(zI,8),re(we,8),re(q,8))};static \u0275dir=T({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[Oe([fB,UI]),be,$e,_m(null)]})}return t})();var hB={provide:Ii,useExisting:Mt(()=>Nr)},Nr=(()=>{class t extends mB{form=null;ngSubmit=new U;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&A("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Oe([hB]),be]})}return t})();var $I=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({})}return t})();function vI(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var rh=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return vI(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Qa(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Kb(r,i)}control(e,i,r){let o={};return this.useNonNullable?(vI(i)?o=i:(o.validators=i,o.asyncValidators=r),new nd(e,Q(b({},o),{nonNullable:!0}))):new nd(e,i,r)}array(e,i,r){let o=e.map(s=>this._createControl(s));return new Xb(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof nd)return e;if(e instanceof Za)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Co=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:id,useValue:e.callSetDisabledState??ih}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[$I]})}return t})(),oh=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:zI,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:id,useValue:e.callSetDisabledState??ih}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[$I]})}return t})();var Rs=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=kt(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=kt(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(G("aria-orientation",r.vertical?"vertical":"horizontal"),H("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2})}return t})();var GI=(()=>{class t{_animationsDisabled=Ae();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&H("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var pB=["*"],gB=`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--mat-list-list-item-container-color, transparent);
  border-radius: var(--mat-list-list-item-container-shape, var(--mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--selected::before, .mdc-list-item.mdc-list-item--selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-leading-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-leading-icon-size, 24px);
  height: var(--mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--with-leading-icon:hover .mdc-list-item__start {
  color: var(--mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--mat-list-list-item-leading-avatar-size, 40px);
  height: var(--mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--mat-list-list-item-trailing-supporting-text-font, var(--mat-sys-label-small-font));
  line-height: var(--mat-list-list-item-trailing-supporting-text-line-height, var(--mat-sys-label-small-line-height));
  font-size: var(--mat-list-list-item-trailing-supporting-text-size, var(--mat-sys-label-small-size));
  font-weight: var(--mat-list-list-item-trailing-supporting-text-weight, var(--mat-sys-label-small-weight));
  letter-spacing: var(--mat-list-list-item-trailing-supporting-text-tracking, var(--mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-trailing-icon-size, 24px);
  height: var(--mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--with-trailing-icon:hover .mdc-list-item__end {
  color: var(--mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-supporting-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-selected-trailing-icon-color, var(--mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--mat-list-list-item-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-list-list-item-label-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-list-list-item-label-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-list-list-item-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-list-list-item-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-list-list-item-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-focus-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--mat-list-list-item-supporting-text-color, var(--mat-sys-on-surface-variant));
  font-family: var(--mat-list-list-item-supporting-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-list-list-item-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-list-list-item-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-list-list-item-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-list-list-item-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-disabled-leading-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-disabled-trailing-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-disabled-label-text-color, var(--mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--mat-list-list-item-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--disabled::before {
  background-color: var(--mat-list-list-item-disabled-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--mat-list-list-item-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--mat-list-list-item-leading-avatar-shape, var(--mat-sys-corner-full));
  background-color: var(--mat-list-list-item-leading-avatar-color, var(--mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
  --mat-focus-indicator-border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--mat-list-active-indicator-color, var(--mat-sys-secondary-container));
}
`,_B=["unscopedContent"];var vB=[[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["mat-divider"]],[["","matListItemAvatar",""],["","matListItemIcon",""]]],bB=["[matListItemTitle]","[matListItemLine]","*","mat-divider","[matListItemAvatar],[matListItemIcon]"];function yB(t,n){t&1&&ie(0,4)}function CB(t,n){if(t&1&&(f(0,"div",11),M(1,"input",12),f(2,"div",13),$t(),f(3,"svg",14),M(4,"path",15),h(),$i(),M(5,"div",16),h()()),t&2){let e=E();H("mdc-checkbox--disabled",e.disabled),p(),x("checked",e.selected)("disabled",e.disabled)}}function wB(t,n){if(t&1&&(f(0,"div",17),M(1,"input",18),f(2,"div",19),M(3,"div",20)(4,"div",21),h()()),t&2){let e=E();H("mdc-radio--disabled",e.disabled),p(),x("checked",e.selected)("disabled",e.disabled)}}function xB(t,n){}function DB(t,n){if(t&1&&(f(0,"span",4),Ge(1,xB,0,0,"ng-template",6),h()),t&2){E();let e=He(3);p(),x("ngTemplateOutlet",e)}}function EB(t,n){}function SB(t,n){if(t&1&&(f(0,"span",5),Ge(1,EB,0,0,"ng-template",6),h()),t&2){E();let e=He(5);p(),x("ngTemplateOutlet",e)}}function IB(t,n){}function kB(t,n){if(t&1&&Ge(0,IB,0,0,"ng-template",6),t&2){E();let e=He(1);x("ngTemplateOutlet",e)}}function MB(t,n){}function TB(t,n){if(t&1&&(f(0,"span",9),Ge(1,MB,0,0,"ng-template",6),h()),t&2){E();let e=He(3);p(),x("ngTemplateOutlet",e)}}function AB(t,n){}function RB(t,n){if(t&1&&(f(0,"span",9),Ge(1,AB,0,0,"ng-template",6),h()),t&2){E();let e=He(5);p(),x("ngTemplateOutlet",e)}}function NB(t,n){}function OB(t,n){if(t&1&&Ge(0,NB,0,0,"ng-template",6),t&2){E();let e=He(1);x("ngTemplateOutlet",e)}}var qI=new y("ListOption"),PB=(()=>{class t{_elementRef=d(F);static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),FB=(()=>{class t{_elementRef=d(F);static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})();var YI=(()=>{class t{_listOption=d(qI,{optional:!0});_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,hostVars:4,hostBindings:function(i,r){i&2&&H("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),LB=(()=>{class t extends YI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[be]})}return t})(),BB=(()=>{class t extends YI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[be]})}return t})(),VB=new y("MAT_LIST_CONFIG"),cy=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=kt(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(kt(e))}_disabled=V(!1);_defaultOptions=d(VB,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,hostVars:1,hostBindings:function(i,r){i&2&&G("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),WI=(()=>{class t{_elementRef=d(F);_ngZone=d(B);_listBase=d(cy,{optional:!0});_platform=d(De);_hostElement;_isButtonElement;_noopAnimations=Ae();_avatars;_icons;set lines(e){this._explicitLines=Hn(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=kt(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(kt(e))}_disabled=V(!1);_subscriptions=new he;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){d(qe).load(Rn);let e=d(zl,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new Is(this,this._ngZone,this._hostElement,this._platform,d(q)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(fn(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,contentQueries:function(i,r,o){if(i&1&&yt(o,LB,4)(o,BB,4),i&2){let s;z(s=$())&&(r._avatars=s),z(s=$())&&(r._icons=s)}},hostVars:4,hostBindings:function(i,r){i&2&&(G("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),H("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var ZI=new y("SelectionList"),od=(()=>{class t extends WI{_selectionList=d(ZI);_changeDetectorRef=d(ge);_lines;_titles;_unscopedContent;selectedChange=new U;togglePosition="after";get color(){return this._color||this._selectionList.color}set color(e){this._color=e}_color;get value(){return this._value}set value(e){this.selected&&e!==this.value&&this._inputsInitialized&&(this.selected=!1),this._value=e}_value;get selected(){return this._selectionList.selectedOptions.isSelected(this)}set selected(e){let i=kt(e);i!==this._selected&&(this._setSelected(i),(i||this._selectionList.multiple)&&this._selectionList._reportValueChange())}_selected=!1;_inputsInitialized=!1;ngOnInit(){let e=this._selectionList;e._value&&e._value.some(r=>e.compareWith(this._value,r))&&this._setSelected(!0);let i=this._selected;Promise.resolve().then(()=>{(this._selected||i)&&(this.selected=!0,this._changeDetectorRef.markForCheck())}),this._inputsInitialized=!0}ngOnDestroy(){super.ngOnDestroy(),this.selected&&Promise.resolve().then(()=>{this.selected=!1})}toggle(){this.selected=!this.selected}focus(){this._hostElement.focus()}getLabel(){return(this._titles?.get(0)?._elementRef.nativeElement||this._unscopedContent?.nativeElement)?.textContent||""}_hasCheckboxAt(e){return this._selectionList.multiple&&this._getTogglePosition()===e}_hasRadioAt(e){return!this._selectionList.multiple&&this._getTogglePosition()===e&&!this._selectionList.hideSingleSelectionIndicator}_hasIconsOrAvatarsAt(e){return this._hasProjected("icons",e)||this._hasProjected("avatars",e)}_hasProjected(e,i){return this._getTogglePosition()!==i&&(e==="avatars"?this._avatars.length!==0:this._icons.length!==0)}_handleBlur(){this._selectionList._onTouched()}_getTogglePosition(){return this.togglePosition||"after"}_setSelected(e){return e===this._selected?!1:(this._selected=e,e?this._selectionList.selectedOptions.select(this):this._selectionList.selectedOptions.deselect(this),this.selectedChange.emit(e),this._changeDetectorRef.markForCheck(),!0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_toggleOnInteraction(){this.disabled||(this._selectionList.multiple?(this.selected=!this.selected,this._selectionList._emitChangeEvent([this])):this.selected||(this.selected=!0,this._selectionList._emitChangeEvent([this])))}_setTabindex(e){this._hostElement.setAttribute("tabindex",e+"")}_hasBothLeadingAndTrailing(){let e=this._hasProjected("avatars","before")||this._hasProjected("icons","before")||this._hasCheckboxAt("before")||this._hasRadioAt("before"),i=this._hasProjected("icons","after")||this._hasProjected("avatars","after")||this._hasCheckboxAt("after")||this._hasRadioAt("after");return e&&i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275cmp=D({type:t,selectors:[["mat-list-option"]],contentQueries:function(i,r,o){if(i&1&&yt(o,FB,5)(o,PB,5),i&2){let s;z(s=$())&&(r._lines=s),z(s=$())&&(r._titles=s)}},viewQuery:function(i,r){if(i&1&&je(_B,5),i&2){let o;z(o=$())&&(r._unscopedContent=o.first)}},hostAttrs:["role","option",1,"mat-mdc-list-item","mat-mdc-list-option","mdc-list-item"],hostVars:27,hostBindings:function(i,r){i&1&&A("blur",function(){return r._handleBlur()})("click",function(){return r._toggleOnInteraction()}),i&2&&(G("aria-selected",r.selected),H("mdc-list-item--selected",r.selected&&!r._selectionList.multiple&&r._selectionList.hideSingleSelectionIndicator)("mdc-list-item--with-leading-avatar",r._hasProjected("avatars","before"))("mdc-list-item--with-leading-icon",r._hasProjected("icons","before"))("mdc-list-item--with-trailing-icon",r._hasProjected("icons","after"))("mat-mdc-list-option-with-trailing-avatar",r._hasProjected("avatars","after"))("mdc-list-item--with-leading-checkbox",r._hasCheckboxAt("before"))("mdc-list-item--with-trailing-checkbox",r._hasCheckboxAt("after"))("mdc-list-item--with-leading-radio",r._hasRadioAt("before"))("mdc-list-item--with-trailing-radio",r._hasRadioAt("after"))("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("mat-accent",r.color!=="primary"&&r.color!=="warn")("mat-warn",r.color==="warn")("_mat-animation-noopable",r._noopAnimations))},inputs:{togglePosition:"togglePosition",color:"color",value:"value",selected:"selected"},outputs:{selectedChange:"selectedChange"},exportAs:["matListOption"],features:[Oe([{provide:WI,useExisting:t},{provide:qI,useExisting:t}]),be],ngContentSelectors:bB,decls:20,vars:4,consts:[["icons",""],["checkbox",""],["radio",""],["unscopedContent",""],[1,"mdc-list-item__start","mat-mdc-list-option-checkbox-before"],[1,"mdc-list-item__start","mat-mdc-list-option-radio-before"],[3,"ngTemplateOutlet"],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mdc-list-item__end"],[1,"mat-focus-indicator"],[1,"mdc-checkbox"],["type","checkbox",1,"mdc-checkbox__native-control",3,"checked","disabled"],[1,"mdc-checkbox__background"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],[1,"mdc-radio"],["type","radio",1,"mdc-radio__native-control",3,"checked","disabled"],[1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"]],template:function(i,r){i&1&&(ke(vB),Ge(0,yB,1,0,"ng-template",null,0,ei)(2,CB,6,4,"ng-template",null,1,ei)(4,wB,5,4,"ng-template",null,2,ei),O(6,DB,2,1,"span",4)(7,SB,2,1,"span",5),O(8,kB,1,1,null,6),f(9,"span",7),ie(10),ie(11,1),f(12,"span",8,3),A("cdkObserveContent",function(){return r._updateItemLines(!0)}),ie(14,2),h()(),O(15,TB,2,1,"span",9)(16,RB,2,1,"span",9),O(17,OB,1,1,null,6),ie(18,3),M(19,"div",10)),i&2&&(p(6),P(r._hasCheckboxAt("before")?6:r._hasRadioAt("before")?7:-1),p(2),P(r._hasIconsOrAvatarsAt("before")?8:-1),p(7),P(r._hasCheckboxAt("after")?15:r._hasRadioAt("after")?16:-1),p(2),P(r._hasIconsOrAvatarsAt("after")?17:-1))},dependencies:[Qi,_S],styles:[`.mat-mdc-list-option-with-trailing-avatar.mdc-list-item, [dir=rtl] .mat-mdc-list-option-with-trailing-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-list-option-with-trailing-avatar .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
  width: 40px;
  height: 40px;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mat-mdc-list-option-with-trailing-avatar .mdc-list-item__end {
  border-radius: 50%;
}

.mat-mdc-list-option .mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mat-mdc-list-option .mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mat-mdc-list-option .mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-list-option .mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}
.mat-mdc-list-option .mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
.mat-mdc-list-option .mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}
.mat-mdc-list-option .mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}
@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-list-option .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-list-option .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-list-option .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-list-option .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-list-option .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-list-option .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark, .mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-list-option .mdc-checkbox__native-control, .mat-mdc-list-option .mdc-radio__native-control {
  display: none;
}

@media (forced-colors: active) {
  .mat-mdc-list-option.mdc-list-item--selected::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  .mat-mdc-list-option.mdc-list-item--selected [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}
`],encapsulation:2})}return t})();var jB={provide:ir,useExisting:Mt(()=>sd),multi:!0},ly=class{source;options;constructor(n,e){this.source=n,this.options=e}},sd=(()=>{class t extends cy{_element=d(F);_ngZone=d(B);_renderer=d(we);_initialized=!1;_keyManager;_listenerCleanups;_destroyed=new S;_isDestroyed=!1;_onChange=e=>{};_items;selectionChange=new U;color="accent";compareWith=(e,i)=>e===i;get multiple(){return this._multiple}set multiple(e){let i=kt(e);i!==this._multiple&&(this._multiple=i,this.selectedOptions=new go(this._multiple,this.selectedOptions.selected))}_multiple=!0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=kt(e)}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;selectedOptions=new go(this._multiple);_value=null;_onTouched=()=>{};_changeDetectorRef=d(ge);constructor(){super(),this._isNonInteractive=!1}ngAfterViewInit(){this._initialized=!0,this._setupRovingTabindex(),this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[this._renderer.listen(this._element.nativeElement,"focusin",this._handleFocusin),this._renderer.listen(this._element.nativeElement,"focusout",this._handleFocusout)]}),this._value&&this._setOptionsFromValues(this._value),this._watchForSelectionChange()}ngOnChanges(e){let i=e.disabled,r=e.disableRipple,o=e.hideSingleSelectionIndicator;(r&&!r.firstChange||i&&!i.firstChange||o&&!o.firstChange)&&this._markOptionsForCheck()}ngOnDestroy(){this._keyManager?.destroy(),this._listenerCleanups?.forEach(e=>e()),this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0}focus(e){this._element.nativeElement.focus(e)}selectAll(){return this._setAllOptionsSelected(!0)}deselectAll(){return this._setAllOptionsSelected(!1)}_reportValueChange(){if(this.options&&!this._isDestroyed){let e=this._getSelectedOptionValues();this._onChange(e),this._value=e}}_emitChangeEvent(e){this.selectionChange.emit(new ly(this,e))}writeValue(e){this._value=e,this.options&&this._setOptionsFromValues(e||[])}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this._markOptionsForCheck()}get disabled(){return this._selectionListDisabled()}set disabled(e){this._selectionListDisabled.set(kt(e)),this._selectionListDisabled()&&this._keyManager?.setActiveItem(-1)}_selectionListDisabled=V(!1);registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}_watchForSelectionChange(){this.selectedOptions.changed.pipe(pe(this._destroyed)).subscribe(e=>{for(let i of e.added)i.selected=!0;for(let i of e.removed)i.selected=!1;this._containsFocus()||this._resetActiveOption()})}_setOptionsFromValues(e){this.options.forEach(i=>i._setSelected(!1)),e.forEach(i=>{let r=this.options.find(o=>o.selected?!1:this.compareWith(o.value,i));r&&r._setSelected(!0)})}_getSelectedOptionValues(){return this.options.filter(e=>e.selected).map(e=>e.value)}_markOptionsForCheck(){this.options&&this.options.forEach(e=>e._markForCheck())}_setAllOptionsSelected(e,i){let r=[];return this.options.forEach(o=>{(!i||!o.disabled)&&o._setSelected(e)&&r.push(o)}),r.length&&this._reportValueChange(),r}get options(){return this._items}_handleKeydown(e){let i=this._keyManager.activeItem;if(i&&(e.keyCode===40||e.keyCode===38)){let r=Xi();if(!this._items.toArray().some(s=>s._elementRef.nativeElement===r)){e.preventDefault(),i.focus();return}}if((e.keyCode===13||e.keyCode===32)&&!this._keyManager.isTyping()&&i&&!i.disabled)e.preventDefault(),i._toggleOnInteraction();else if(e.keyCode===65&&this.multiple&&!this._keyManager.isTyping()&&Ct(e,"ctrlKey","metaKey")){let r=this.options.some(o=>!o.disabled&&!o.selected);e.preventDefault(),this._emitChangeEvent(this._setAllOptionsSelected(r,!0))}else this._keyManager.onKeydown(e)}_handleFocusout=()=>{setTimeout(()=>{this._containsFocus()||this._resetActiveOption()})};_handleFocusin=e=>{let i=this._items.toArray().findIndex(r=>r._elementRef.nativeElement.contains(e.target));i>-1?this._setActiveOption(i):this._resetActiveOption()};_setupRovingTabindex(){this._keyManager=new Mr(this._items).withHomeAndEnd().withTypeAhead().withWrap().skipPredicate(()=>!1),this._resetActiveOption(),this._keyManager.change.subscribe(e=>this._setActiveOption(e)),this._items.changes.pipe(pe(this._destroyed)).subscribe(()=>{let e=this._keyManager.activeItem;(!e||this._items.toArray().indexOf(e)===-1)&&this._resetActiveOption()})}_setActiveOption(e){this._items.forEach((i,r)=>{let o=-1;!this.disabled&&r===e&&(o=0),i._setTabindex(o)}),this._keyManager.updateActiveItem(e)}_resetActiveOption(){if(this.disabled){let i=this._items.find(o=>o.selected)||this._items.first,r=i?this._items.toArray().indexOf(i):-1;this._setActiveOption(r);return}let e=this._items.find(i=>i.selected&&!i.disabled)||this._items.first;this._setActiveOption(e?this._items.toArray().indexOf(e):-1)}_containsFocus(){let e=Xi();return e&&this._element.nativeElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-selection-list"]],contentQueries:function(i,r,o){if(i&1&&yt(o,od,5),i&2){let s;z(s=$())&&(r._items=s)}},hostAttrs:["role","listbox",1,"mat-mdc-selection-list","mat-mdc-list-base","mdc-list"],hostVars:1,hostBindings:function(i,r){i&1&&A("keydown",function(s){return r._handleKeydown(s)}),i&2&&G("aria-multiselectable",r.multiple)},inputs:{color:"color",compareWith:"compareWith",multiple:"multiple",hideSingleSelectionIndicator:"hideSingleSelectionIndicator",disabled:"disabled"},outputs:{selectionChange:"selectionChange"},exportAs:["matSelectionList"],features:[Oe([jB,{provide:cy,useExisting:t},{provide:ZI,useExisting:t}]),be,$e],ngContentSelectors:pB,decls:1,vars:0,template:function(i,r){i&1&&(ke(),ie(0))},styles:[gB],encapsulation:2})}return t})();function HB(t,n){if(t&1&&(f(0,"mat-list-option",5),v(1),h()),t&2){let e=n.$implicit;x("value",e),p(),oe(" ",e," ")}}function UB(t,n){if(t&1&&(f(0,"mat-list-option",5),v(1),h()),t&2){let e=n.$implicit;x("value",e),p(),oe(" ",e," ")}}var sh=class t{shopService=d(lo);dialogRef=d(Ql);data=d(Wb);selectedBrands=this.data.selectedBrands;selectedTypes=this.data.selectedTypes;applyFilters(){this.dialogRef.close({selectedBrands:this.selectedBrands,selectedTypes:this.selectedTypes})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-filter-dialog"]],decls:20,vars:4,consts:[[1,"text-3xl","text-center","font-semibold","mb-4"],[1,"flex","p-4"],[1,"w-1/2"],[1,"text-lg","font-semibold","mb-2"],[3,"ngModelChange","ngModel","multiple"],[3,"value"],[1,"flex","justify-end","p-4"],["mat-flat-button","","color","primary",3,"click"]],template:function(e,i){e&1&&(f(0,"div")(1,"h3",0),v(2,"Filters"),h(),M(3,"mat-divider"),f(4,"div",1)(5,"div",2)(6,"h4",3),v(7,"Brands"),h(),f(8,"mat-selection-list",4),Dr("ngModelChange",function(o){return io(i.selectedBrands,o)||(i.selectedBrands=o),o}),at(9,HB,2,2,"mat-list-option",5,ym),h(),nn(),h(),f(11,"div",2)(12,"h4",3),v(13,"Types"),h(),f(14,"mat-selection-list",4),Dr("ngModelChange",function(o){return io(i.selectedTypes,o)||(i.selectedTypes=o),o}),at(15,UB,2,2,"mat-list-option",5,ym),h(),nn(),h()(),f(17,"div",6)(18,"button",7),A("click",function(){return i.applyFilters()}),v(19,"Apply Filters"),h()()()),e&2&&(p(8),xr("ngModel",i.selectedBrands),x("multiple",!0),rn(),p(),ct(i.shopService.brandsList),p(5),xr("ngModel",i.selectedTypes),x("multiple",!0),rn(),p(),ct(i.shopService.typesList))},dependencies:[Rs,sd,od,it,Co,or,As],encapsulation:2})};var zB=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],$B=["mat-icon, [matMenuItemIcon]","*"];function GB(t,n){t&1&&($t(),f(0,"svg",2),M(1,"polygon",3),h())}var WB=["*"];function qB(t,n){if(t&1){let e=lt();Ve(0,"div",0),pa("click",function(){Ee(e);let r=E();return Se(r.closed.emit("click"))})("animationstart",function(r){Ee(e);let o=E();return Se(o._onAnimationStart(r.animationName))})("animationend",function(r){Ee(e);let o=E();return Se(o._onAnimationDone(r.animationName))})("animationcancel",function(r){Ee(e);let o=E();return Se(o._onAnimationDone(r.animationName))}),Ve(1,"div",1),ie(2),Qe()()}if(t&2){let e=E();jt(e._classList),H("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),ht("id",e.panelId),G("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var uy=new y("MAT_MENU_PANEL"),ad=(()=>{class t{_elementRef=d(F);_document=d(Z);_focusMonitor=d(An);_parentMenu=d(uy,{optional:!0});_changeDetectorRef=d(ge);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new S;_focused=new S;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(qe).load(Rn),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&A("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(G("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),H("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",L],disableRipple:[2,"disableRipple","disableRipple",L]},exportAs:["matMenuItem"],ngContentSelectors:$B,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(ke(zB),ie(0),f(1,"span",0),ie(2,1),h(),M(3,"div",1),O(4,GB,2,0,":svg:svg",2)),i&2&&(p(3),x("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),p(),P(r._triggersSubmenu?4:-1))},dependencies:[er],encapsulation:2})}return t})();var YB=new y("MatMenuContent");var ZB=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),dy="_mat-menu-enter",ah="_mat-menu-exit",wo=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(ge);_injector=d(q);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Ae();_allItems;_directDescendantItems=new En;_classList={};_panelAnimationState="void";_animationDone=new S;_isAnimating=V(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;get panelClass(){return this._previousPanelClass}set panelClass(e){let i=this._previousPanelClass,r=b({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass="";get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new U;close=this.closed;panelId=d(Te).getId("mat-menu-panel-");constructor(){let e=d(ZB);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Mr(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(et(this._directDescendantItems),pt(e=>fn(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(et(this._directDescendantItems),pt(i=>fn(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Ct(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=vt(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=Q(b({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===ah;(i||e===dy)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===dy||e===ah)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(ah),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?dy:ah)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(et(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&yt(o,YB,5)(o,ad,5)(o,ad,4),i&2){let s;z(s=$())&&(r.lazyContent=s.first),z(s=$())&&(r._allItems=s),z(s=$())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&je(gt,5),i&2){let o;z(o=$())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&G("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",L],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:L(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Oe([{provide:uy,useExisting:t}])],ngContentSelectors:WB,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(ke(),ha(0,qB,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2})}return t})(),QB=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(q);return()=>ho(t)}});var Xa=new WeakMap,KB=(()=>{class t{_canHaveBackdrop;_element=d(F);_viewContainerRef=d(Vt);_menuItemInstance=d(ad,{optional:!0,self:!0});_dir=d(Zt,{optional:!0});_focusMonitor=d(An);_ngZone=d(B);_injector=d(q);_scrollStrategy=d(QB);_changeDetectorRef=d(ge);_animationsDisabled=Ae();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=he.EMPTY;_menuCloseSubscription=he.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e?(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})):this._destroyMenu(),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(uy,{optional:!0});this._parentMaterialMenu=i instanceof wo?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Xa.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Xa.get(i);Xa.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof wo&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(pe(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof wo&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(mt(1)).subscribe(()=>{i.detach(),Xa.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Xa.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=nr(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof wo&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Si({positionStrategy:Wa(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[c,l]=[s,a],[u,m]=[r,o],g=0;if(this._triggersSubmenu()){if(m=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}g=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(c=s==="top"?"bottom":"top",l=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:g},{originX:o,originY:c,overlayX:m,overlayY:s,offsetY:g},{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:-g},{originX:o,originY:l,overlayX:m,overlayY:a,offsetY:-g}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:W(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(Ie(s=>this._menuOpen&&s!==this._menuItemInstance)):W();return fn(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Un(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Xa.get(e)===this}_triggerIsAriaDisabled(){return L(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){el()};static \u0275dir=T({type:t})}return t})(),ch=(()=>{class t extends KB{_cleanupTouchstart;_hoverSubscription=he.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new U;onMenuOpen=this.menuOpened;menuClosed=new U;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(we);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{Cs(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){ys(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&A("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&G("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu?.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[be]})}return t})();var lh=class{brands=[];types=[];sort="name";pageNumber=1;pageSize=20;search=""};var my=class{_box;_destroyed=new S;_resizeSubject=new S;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new le(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ie(e=>e.some(i=>i.target===n)),Jd({bufferSize:1,refCount:!0}),pe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},QI=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(B);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new my(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var XB=["notch"],JB=["*"],KI=["iconPrefixContainer"],XI=["textPrefixContainer"],JI=["iconSuffixContainer"],ek=["textSuffixContainer"],eV=["textField"],tV=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],nV=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function iV(t,n){t&1&&M(0,"span",21)}function rV(t,n){if(t&1&&(f(0,"label",20),ie(1,1),O(2,iV,1,0,"span",21),h()),t&2){let e=E(2);x("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),G("for",e._control.disableAutomaticLabeling?null:e._control.id),p(2),P(!e.hideRequiredMarker&&e._control.required?2:-1)}}function oV(t,n){if(t&1&&O(0,rV,3,5,"label",20),t&2){let e=E();P(e._hasFloatingLabel()?0:-1)}}function sV(t,n){t&1&&M(0,"div",7)}function aV(t,n){}function cV(t,n){if(t&1&&Ge(0,aV,0,0,"ng-template",13),t&2){E(2);let e=He(1);x("ngTemplateOutlet",e)}}function lV(t,n){if(t&1&&(f(0,"div",9),O(1,cV,1,1,null,13),h()),t&2){let e=E();x("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),p(),P(e._forceDisplayInfixLabel()?-1:1)}}function dV(t,n){t&1&&(f(0,"div",10,2),ie(2,2),h())}function uV(t,n){t&1&&(f(0,"div",11,3),ie(2,3),h())}function mV(t,n){}function fV(t,n){if(t&1&&Ge(0,mV,0,0,"ng-template",13),t&2){E();let e=He(1);x("ngTemplateOutlet",e)}}function hV(t,n){t&1&&(f(0,"div",14,4),ie(2,4),h())}function pV(t,n){t&1&&(f(0,"div",15,5),ie(2,5),h())}function gV(t,n){t&1&&M(0,"div",16)}function _V(t,n){t&1&&(f(0,"div",18),ie(1,6),h())}function vV(t,n){if(t&1&&(f(0,"mat-hint",22),v(1),h()),t&2){let e=E(2);x("id",e._hintLabelId),p(),X(e.hintLabel)}}function bV(t,n){if(t&1&&(f(0,"div",19),O(1,vV,2,2,"mat-hint",22),ie(2,7),M(3,"div",23),ie(4,8),h()),t&2){let e=E();p(),P(e.hintLabel?1:-1)}}var ai=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-label"]]})}return t})(),ak=new y("MatError"),mh=(()=>{class t{id=d(Te).getId("mat-mdc-error-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&ht("id",r.id)},inputs:{id:"id"},features:[Oe([{provide:ak,useExisting:t}])]})}return t})(),uh=(()=>{class t{align="start";id=d(Te).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(ht("id",r.id),G("align",null),H("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),yV=new y("MatPrefix");var CV=new y("MatSuffix");var ck=new y("FloatingLabelParent"),tk=(()=>{class t{_elementRef=d(F);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(QI);_ngZone=d(B);_parent=d(ck);_resizeSubscription=new he;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return wV(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&H("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function wV(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var nk="mdc-line-ripple--active",dh="mdc-line-ripple--deactivating",ik=(()=>{class t{_elementRef=d(F);_cleanupTransitionEnd;constructor(){let e=d(B),i=d(we);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(dh),e.add(nk)}deactivate(){this._elementRef.nativeElement.classList.add(dh)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(dh);e.propertyName==="opacity"&&r&&i.remove(nk,dh)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),rk=(()=>{class t{_elementRef=d(F);_ngZone=d(B);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&je(XB,5),i&2){let o;z(o=$())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&H("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:JB,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(ke(),In(0,"div",1),Ve(1,"div",2,0),ie(3),Qe(),In(4,"div",3))},encapsulation:2})}return t})(),cd=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t})}return t})();var ld=new y("MatFormField"),xV=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),ok="fill",DV="auto",sk="fixed",EV="translateY(-50%)",ci=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(ge);_platform=d(De);_idGenerator=d(Te);_ngZone=d(B);_defaults=d(xV,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ol("iconPrefixContainer");_textPrefixContainerSignal=ol("textPrefixContainer");_iconSuffixContainerSignal=ol("iconSuffixContainer");_textSuffixContainerSignal=ol("textSuffixContainer");_prefixSuffixContainers=dt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=lD(ai);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=kt(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||DV}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||ok;this._appearanceSignal.set(i)}_appearanceSignal=V(ok);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||sk}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||sk}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new S;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ae();constructor(){let e=this._defaults,i=d(Zt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Kn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=dt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(et([void 0,void 0]),Y(()=>[i.errorState,i.userAriaDescribedBy]),Xd(),Ie(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(pe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),fn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){bv({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=dt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",m=`${s+a}px`,_=`calc(${u} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,C=`var(--mat-mdc-form-field-label-transform, ${EV} translateX(${_}))`,I=s+a+c+l;return[C,I]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Cm(o,r._labelChild,ai,5),yt(o,cd,5)(o,yV,5)(o,CV,5)(o,ak,5)(o,uh,5)),i&2){xm();let s;z(s=$())&&(r._formFieldControl=s.first),z(s=$())&&(r._prefixChildren=s),z(s=$())&&(r._suffixChildren=s),z(s=$())&&(r._errorChildren=s),z(s=$())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(wm(r._iconPrefixContainerSignal,KI,5)(r._textPrefixContainerSignal,XI,5)(r._iconSuffixContainerSignal,JI,5)(r._textSuffixContainerSignal,ek,5),je(eV,5)(KI,5)(XI,5)(JI,5)(ek,5)(tk,5)(rk,5)(ik,5)),i&2){xm(4);let o;z(o=$())&&(r._textField=o.first),z(o=$())&&(r._iconPrefixContainer=o.first),z(o=$())&&(r._textPrefixContainer=o.first),z(o=$())&&(r._iconSuffixContainer=o.first),z(o=$())&&(r._textSuffixContainer=o.first),z(o=$())&&(r._floatingLabel=o.first),z(o=$())&&(r._notchedOutline=o.first),z(o=$())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&H("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Oe([{provide:ld,useExisting:t},{provide:ck,useExisting:t}])],ngContentSelectors:nV,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(ke(tV),Ge(0,oV,1,1,"ng-template",null,0,ei),f(2,"div",6,1),A("click",function(s){return r._control.onContainerClick(s)}),O(4,sV,1,0,"div",7),f(5,"div",8),O(6,lV,2,2,"div",9),O(7,dV,3,0,"div",10),O(8,uV,3,0,"div",11),f(9,"div",12),O(10,fV,1,1,null,13),ie(11),h(),O(12,hV,3,0,"div",14),O(13,pV,3,0,"div",15),h(),O(14,gV,1,0,"div",16),h(),f(15,"div",17),O(16,_V,2,0,"div",18)(17,bV,5,1,"div",19),h()),i&2){let o;p(2),H("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),p(2),P(!r._hasOutline()&&!r._control.disabled?4:-1),p(2),P(r._hasOutline()?6:-1),p(),P(r._hasIconPrefix?7:-1),p(),P(r._hasTextPrefix?8:-1),p(2),P(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),p(2),P(r._hasTextSuffix?12:-1),p(),P(r._hasIconSuffix?13:-1),p(),P(r._hasOutline()?-1:14),p(),H("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();p(),P((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[tk,rk,Qi,ik,uh],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var fh=new y("");var SV=["text"],IV=[[["mat-icon"]],"*"],kV=["mat-icon","*"];function MV(t,n){if(t&1&&M(0,"mat-pseudo-checkbox",1),t&2){let e=E();x("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function TV(t,n){if(t&1&&M(0,"mat-pseudo-checkbox",3),t&2){let e=E();x("disabled",e.disabled)}}function AV(t,n){if(t&1&&(f(0,"span",4),v(1),h()),t&2){let e=E();p(),oe("(",e.group.label,")")}}var hy=new y("MAT_OPTION_PARENT_COMPONENT"),py=new y("MatOptgroup");var fy=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},hh=(()=>{class t{_element=d(F);_changeDetectorRef=d(ge);_parent=d(hy,{optional:!0});group=d(py,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Te).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=V(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new U;_text;_stateChanges=new S;constructor(){let e=d(qe);e.load(Rn),e.load(Ji),this._signalDisableRipple=!!this._parent&&Dn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ct(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new fy(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&je(SV,7),i&2){let o;z(o=$())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&A("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(ht("id",r.id),G("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),H("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",L]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:kV,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(ke(IV),O(0,MV,1,2,"mat-pseudo-checkbox",1),ie(1),f(2,"span",2,0),ie(4,1),h(),O(5,TV,1,1,"mat-pseudo-checkbox",3),O(6,AV,2,1,"span",4),M(7,"div",5)),i&2&&(P(r.multiple?0:-1),p(5),P(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),p(),P(r.group&&r.group._inert?6:-1),p(),x("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[GI,er],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function lk(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function dk(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var xo=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Ja=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?Dn(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var OV=["trigger"],PV=["panel"],FV=[[["mat-select-trigger"]],"*"],LV=["mat-select-trigger","*"];function BV(t,n){if(t&1&&(f(0,"span",4),v(1),h()),t&2){let e=E();p(),X(e.placeholder)}}function VV(t,n){t&1&&ie(0)}function jV(t,n){if(t&1&&(f(0,"span",11),v(1),h()),t&2){let e=E(2);p(),X(e.triggerValue)}}function HV(t,n){if(t&1&&(f(0,"span",5),O(1,VV,1,0)(2,jV,2,1,"span",11),h()),t&2){let e=E();p(),P(e.customTrigger?1:2)}}function UV(t,n){if(t&1){let e=lt();f(0,"div",12,1),A("keydown",function(r){Ee(e);let o=E();return Se(o._handleKeydown(r))}),ie(2,1),h()}if(t&2){let e=E();jt(e.panelClass),H("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),G("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var zV=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(q);return()=>ho(t)}}),$V=new y("MAT_SELECT_CONFIG"),GV=new y("MatSelectTrigger"),gy=class{source;value;constructor(n,e){this.source=n,this.value=e}},uk=(()=>{class t{_viewportRuler=d(mo);_changeDetectorRef=d(ge);_elementRef=d(F);_dir=d(Zt,{optional:!0});_idGenerator=d(Te);_renderer=d(we);_parentFormField=d(ld,{optional:!0});ngControl=d(si,{self:!0,optional:!0});_liveAnnouncer=d(Bl);_defaultOptions=d($V,{optional:!0});_animationsDisabled=Ae();_popoverLocation;_initialized=new S;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=lk(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=dk(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new gy(this,e)}_scrollStrategyFactory=d(zV);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new S;_errorStateTracker;stateChanges=new S;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=V(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(zn.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=hi(()=>{let e=this.options;return e?e.changes.pipe(et(e),pt(()=>fn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(pt(()=>this.optionSelectionChanges))});openedChange=new U;_openedStream=this.openedChange.pipe(Ie(e=>e),Y(()=>{}));_closedStream=this.openedChange.pipe(Ie(e=>!e),Y(()=>{}));selectionChange=new U;valueChange=new U;constructor(){let e=d(xo),i=d(Rr,{optional:!0}),r=d(Nr,{optional:!0}),o=d(new ti("tabindex"),{optional:!0}),s=d(ql,{optional:!0}),a=d(fh,{optional:!0,self:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ja(e,a||this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new go(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(pe(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(pe(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(et(null),pe(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(mt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!Ct(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!Ct(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ct(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ga?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Hl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=fn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(pe(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),fn(...this.options.map(i=>i._stateChanges)).pipe(pe(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Qt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&yt(o,GV,5)(o,hh,5)(o,py,5),i&2){let s;z(s=$())&&(r.customTrigger=s.first),z(s=$())&&(r.options=s),z(s=$())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&je(OV,5)(PV,5)(zf,5),i&2){let o;z(o=$())&&(r.trigger=o.first),z(o=$())&&(r.panel=o.first),z(o=$())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&A("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(G("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),H("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",L],disableRipple:[2,"disableRipple","disableRipple",L],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Et(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",L],placeholder:"placeholder",required:[2,"required","required",L],multiple:[2,"multiple","multiple",L],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",L],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Et],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",L]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Oe([{provide:cd,useExisting:t},{provide:hy,useExisting:t}]),$e],ngContentSelectors:LV,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(ke(FV),f(0,"div",2,0),A("click",function(){return r.open()}),f(3,"div",3),O(4,BV,2,1,"span",4)(5,HV,3,1,"span",5),h(),f(6,"div",6)(7,"div",7),$t(),f(8,"svg",8),M(9,"path",9),h()()()(),Ge(10,UV,3,16,"ng-template",10),A("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=He(1);p(3),G("id",r._valueId),p(),P(r.empty?4:5),p(6),x("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ga,zf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var WV=["tooltip"],qV=20;var YV=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(q);return()=>ho(t,{scrollThrottle:qV})}}),ZV=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var mk="tooltip-panel",QV={passive:!0},KV=8,XV=8,JV=24,ej=200,fk=(()=>{class t{_elementRef=d(F);_ngZone=d(B);_platform=d(De);_ariaDescriber=d(Tf);_focusMonitor=d(An);_dir=d(Zt);_injector=d(q);_viewContainerRef=d(Vt);_mediaMatcher=d(La);_document=d(Z);_renderer=d(we);_animationsDisabled=Ae();_defaultOptions=d(ZV,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=tj;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=kt(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=kt(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=Hn(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=Hn(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new S;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=KV}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(pe(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Ei(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(pe(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof F)return this._overlayRef;this._detach()}let i=this._injector.get(Ua).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${mk}`,o=Wa(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(pe(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=nr(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(YV)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(pe(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(pe(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(pe(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(pe(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(b(b({},r.main),o.main)),this._addOffset(b(b({},r.fallback),o.fallback))])}_addOffset(e){let i=XV,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),vt(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let c=`${this._cssClassPrefix}-${mk}-`;a.removePanelClass(c+this._currentPosition),a.addPanelClass(c+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,QV))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||vt({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!Ct(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&H("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),tj=(()=>{class t{_changeDetectorRef=d(ge);_elementRef=d(F);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Ae();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new S;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>JV&&e.width>=ej}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&je(WV,7),i&2){let o;z(o=$())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&A("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(Ve(0,"div",1,0),pa("animationend",function(s){return r._handleAnimationEnd(s)}),Ve(2,"div",2),v(3),Qe()()),i&2&&(jt(r.tooltipClass),H("mdc-tooltip--multiline",r._isMultiline),p(3),X(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2})}return t})();function nj(t,n){if(t&1&&(f(0,"mat-option",17),v(1),h()),t&2){let e=n.$implicit;x("value",e),p(),oe(" ",e," ")}}function ij(t,n){if(t&1){let e=lt();f(0,"mat-form-field",14)(1,"mat-select",16,0),A("selectionChange",function(r){Ee(e);let o=E(2);return Se(o._changePageSize(r.value))}),at(3,nj,2,2,"mat-option",17,Cr),h(),f(5,"div",18),A("click",function(){Ee(e);let r=He(2);return Se(r.open())}),h()()}if(t&2){let e=E(2);x("appearance",e._formFieldAppearance)("color",e.color),p(),x("value",e.pageSize)("disabled",e.disabled),bm("aria-labelledby",e._pageSizeLabelId),x("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),p(2),ct(e._displayedPageSizeOptions)}}function rj(t,n){if(t&1&&(f(0,"div",15),v(1),h()),t&2){let e=E(2);p(),X(e.pageSize)}}function oj(t,n){if(t&1&&(f(0,"div",3)(1,"div",13),v(2),h(),O(3,ij,6,7,"mat-form-field",14),O(4,rj,2,1,"div",15),h()),t&2){let e=E();p(),G("id",e._pageSizeLabelId),p(),oe(" ",e._intl.itemsPerPageLabel," "),p(),P(e._displayedPageSizeOptions.length>1?3:-1),p(),P(e._displayedPageSizeOptions.length<=1?4:-1)}}function sj(t,n){if(t&1){let e=lt();f(0,"button",19),A("click",function(){Ee(e);let r=E();return Se(r._buttonClicked(0,r._previousButtonsDisabled()))}),$t(),f(1,"svg",8),M(2,"path",20),h()()}if(t&2){let e=E();x("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),G("aria-label",e._intl.firstPageLabel)}}function aj(t,n){if(t&1){let e=lt();f(0,"button",21),A("click",function(){Ee(e);let r=E();return Se(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),$t(),f(1,"svg",8),M(2,"path",22),h()()}if(t&2){let e=E();x("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),G("aria-label",e._intl.lastPageLabel)}}var cj=(()=>{class t{changes=new S;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,s=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${s} of ${r}`};static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),lj=50;var dj=new y("MAT_PAGINATOR_DEFAULT_OPTIONS"),hk=(()=>{class t{_intl=d(cj);_changeDetectorRef=d(ge);_formFieldAppearance;_pageSizeLabelId=d(Te).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new Ri(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>Et(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new U;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=d(dj,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:s,showFirstLastButtons:a}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),s!=null&&(this.hidePageSize=s),a!=null&&(this.showFirstLastButtons=a)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:lj),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",Et],length:[2,"length","length",Et],pageSize:[2,"pageSize","pageSize",Et],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",L],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",L],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",L]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2),O(2,oj,5,4,"div",3),f(3,"div",4)(4,"div",5),v(5),h(),O(6,sj,3,5,"button",6),f(7,"button",7),A("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),$t(),f(8,"svg",8),M(9,"path",9),h()(),$i(),f(10,"button",10),A("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),$t(),f(11,"svg",8),M(12,"path",11),h()(),O(13,aj,3,5,"button",12),h()()()),i&2&&(p(2),P(r.hidePageSize?-1:2),p(3),oe(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),p(),P(r.showFirstLastButtons?6:-1),p(),x("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),G("aria-label",r._intl.previousPageLabel),p(3),x("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),G("aria-label",r._intl.nextPageLabel),p(3),P(r.showFirstLastButtons?13:-1))},dependencies:[ci,uk,hh,Lb,fk],styles:[`.mat-mdc-paginator {
  display: block;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-paginator-container-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-paginator-container-background-color, var(--mat-sys-surface));
  font-family: var(--mat-paginator-container-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-paginator-container-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-paginator-container-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-paginator-container-text-weight, var(--mat-sys-body-small-weight));
  letter-spacing: var(--mat-paginator-container-text-tracking, var(--mat-sys-body-small-tracking));
  --mat-form-field-container-height: var(--mat-paginator-form-field-container-height, 40px);
  --mat-form-field-container-vertical-padding: var(--mat-paginator-form-field-container-vertical-padding, 8px);
}
.mat-mdc-paginator .mat-mdc-select-value {
  font-size: var(--mat-paginator-select-trigger-text-size, var(--mat-sys-body-small-size));
}
.mat-mdc-paginator .mat-mdc-form-field-subscript-wrapper {
  display: none;
}
.mat-mdc-paginator .mat-mdc-select {
  line-height: 1.5;
}

.mat-mdc-paginator-outer-container {
  display: flex;
}

.mat-mdc-paginator-container {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 8px;
  flex-wrap: wrap;
  width: 100%;
  min-height: var(--mat-paginator-container-size, 56px);
}

.mat-mdc-paginator-page-size {
  display: flex;
  align-items: baseline;
  margin-right: 8px;
}
[dir=rtl] .mat-mdc-paginator-page-size {
  margin-right: 0;
  margin-left: 8px;
}

.mat-mdc-paginator-page-size-label {
  margin: 0 4px;
}

.mat-mdc-paginator-page-size-select {
  margin: 0 4px;
  width: var(--mat-paginator-page-size-select-width, 84px);
}

.mat-mdc-paginator-range-label {
  margin: 0 32px 0 24px;
}

.mat-mdc-paginator-range-actions {
  display: flex;
  align-items: center;
}

.mat-mdc-paginator-icon {
  display: inline-block;
  width: 28px;
  fill: var(--mat-paginator-enabled-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon {
  fill: var(--mat-paginator-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
[dir=rtl] .mat-mdc-paginator-icon {
  transform: rotate(180deg);
}

@media (forced-colors: active) {
  .mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon,
  .mat-mdc-paginator-icon {
    fill: currentColor;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button {
    outline: solid 1px;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button[aria-disabled] {
    color: GrayText;
  }
}
.mat-mdc-paginator-touch-target {
  display: var(--mat-paginator-touch-target-display, block);
  position: absolute;
  top: 50%;
  left: 50%;
  width: var(--mat-paginator-page-size-select-width, 84px);
  height: var(--mat-paginator-page-size-select-touch-target-height, 48px);
  background-color: transparent;
  transform: translate(-50%, -50%);
  cursor: pointer;
}
`],encapsulation:2})}return t})();var uj=(t,n)=>n.id,mj=(t,n)=>n.value;function fj(t,n){if(t&1&&M(0,"app-product-item",13),t&2){let e=n.$implicit;x("product",e)}}function hj(t,n){if(t&1&&(f(0,"mat-list-option",15),v(1),h()),t&2){let e=n.$implicit,i=E(2);x("value",e.value)("selected",i.shopParams.sort===e.value),p(),oe(" ",e.name," ")}}function pj(t,n){if(t&1){let e=lt();f(0,"div",2)(1,"div",3)(2,"mat-paginator",4),A("page",function(r){Ee(e);let o=E();return Se(o.onPageChange(r))}),h(),f(3,"form",5,0),A("ngSubmit",function(){Ee(e);let r=E();return Se(r.onSearchChange())}),f(5,"input",6),Dr("ngModelChange",function(r){Ee(e);let o=E();return io(o.shopParams.search,r)||(o.shopParams.search=r),Se(r)}),h(),nn(),f(6,"button",7)(7,"mat-icon",8),v(8,"search"),h()()(),f(9,"div",9)(10,"button",10),A("click",function(){Ee(e);let r=E();return Se(r.openFilterDialog())}),f(11,"mat-icon"),v(12,"filter_list"),h(),v(13," Filter "),h(),f(14,"button",11)(15,"mat-icon"),v(16,"swap_vert"),h(),v(17," Sort "),h()()(),f(18,"div",12),at(19,fj,1,1,"app-product-item",13,uj),h()(),f(21,"mat-menu",null,1)(23,"mat-selection-list",14),A("selectionChange",function(r){Ee(e);let o=E();return Se(o.applySortChange(r))}),at(24,hj,2,3,"mat-list-option",15,mj),h()()}if(t&2){let e=He(22),i=E();p(2),x("length",i.products?.count||0)("pageSize",i.shopParams.pageSize)("pageIndex",i.shopParams.pageNumber-1)("pageSizeOptions",i.pageSizeOptions)("showFirstLastButtons",!0),p(3),xr("ngModel",i.shopParams.search),rn(),p(9),x("matMenuTriggerFor",e),p(5),ct(i.products?.data),p(4),x("multiple",!1),p(),ct(i.selectedOptions)}}var ph=class t{shopService=d(lo);dialogService=d(dI);products;cdr=d(ge);selectedOptions=[{name:"Alphabetical",value:"name"},{name:"Price: Low to High",value:"price_asc"},{name:"Price: High to Low",value:"price_desc"}];pageSizeOptions=[5,10,20,25];shopParams=new lh;ngOnInit(){this.ngInitialize()}ngInitialize(){this.shopService.getBrands(),this.shopService.getTypes(),this.getProductsData()}openFilterDialog(){this.dialogService.open(sh,{width:"500px",data:{selectedBrands:this.shopParams.brands,selectedTypes:this.shopParams.types}}).afterClosed().subscribe(e=>{e&&(this.shopParams.brands=e.selectedBrands,this.shopParams.types=e.selectedTypes,this.shopParams.pageNumber=1,this.getProductsData())})}applySortChange(n){let e=n.options[0].value;e!==this.shopParams.sort&&(this.shopParams.sort=e,this.shopParams.pageNumber=1,console.log("Selected sort option:",this.shopParams.sort),this.getProductsData())}onPageChange(n){this.shopParams.pageNumber=n.pageIndex+1,this.shopParams.pageSize=n.pageSize,this.getProductsData()}onSearchChange(){this.shopParams.pageNumber=1,console.log("Search term:",this.shopParams.search),this.getProductsData()}getProductsData(){this.shopService.getProducts(this.shopParams).subscribe({next:n=>{this.products=n,this.cdr.detectChanges()},error:n=>{console.error("Error fetching products:",n)}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-shop"]],decls:1,vars:1,consts:[["searchForm","ngForm"],["sortMenu","matMenu"],[1,"flex","flex-col","gap-3"],[1,"flex","justify-between"],["aria-label","Select page",3,"page","length","pageSize","pageIndex","pageSizeOptions","showFirstLastButtons"],[1,"relative","flex","items-center","w-full","max-w-md","mx-4",3,"ngSubmit"],["type","search","placeholder","Search","name","search",1,"block","w-full","p-4","text-sm","text-gray-900","border","border-gray-300","rounded-lg","bg-gray-50","focus:border-blue-500","focus:ring-blue-500",3,"ngModelChange","ngModel"],["mat-icon-button","","type","submit",1,"absolute","inset-y-0","right-8","top-2","flex","items-center","pl-3"],[1,"text-gray-500"],[1,"flex","gap-3"],["matButton","outlined","color","primary",1,"custom-shadow-btn",3,"click"],["matButton","outlined","color","primary",1,"custom-shadow-btn",3,"matMenuTriggerFor"],[1,"grid","grid-cols-5","gap-4"],[3,"product"],[3,"selectionChange","multiple"],[3,"value","selected"]],template:function(e,i){e&1&&O(0,pj,26,8),e&2&&P(i.products?0:-1)},dependencies:[Pf,it,ln,wo,sd,od,ch,hk,Co,yo,rr,or,bo,As,Rr],encapsulation:2})};var gh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-home"]],decls:8,vars:0,consts:[[1,"max-w-screen-2xl","mx-auto","px-4","mt-32"],[1,"flex","flex-col","items-center","py-16","justify-center","mt-20","rounded-2xl","shadow-xl","relative"],["src","../images/hero1.jpg","alt","footwear resort image",1,"absolute","inset-0","w-full","h-full","object-cover","rounded-2xl"],[1,"flex","flex-col","p-8","rounded-2xl","items-center","relative"],[1,"my-6","font-extrabold","text-white","text-6xl"],["routerLink","/shop",1,"bg-gradient-to-r","from-blue-600","to-cyan-500","font-semibold","text-2xl","text-white","rounded-2xl","px-8","py-4","border-2","border-transparent","mt-8"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1),M(2,"img",2),f(3,"div",3)(4,"h1",4),v(5," Welcome to SkiNet! "),h(),f(6,"button",5),v(7," Go to shop "),h()()()())},dependencies:[Nt],encapsulation:2})};var gj=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),_j={passive:!0},pk=(()=>{class t{_platform=d(De);_ngZone=d(B);_renderer=d(At).createRenderer(null,null);_styleLoader=d(qe);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return xt;this._styleLoader.load(gj);let i=wn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new S,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,_j)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=wn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var gk=new y("MAT_INPUT_VALUE_ACCESSOR");var vj=["button","checkbox","file","hidden","image","radio","range","reset","submit"],bj=new y("MAT_INPUT_CONFIG"),Do=(()=>{class t{_elementRef=d(F);_platform=d(De);ngControl=d(si,{optional:!0,self:!0});_autofillMonitor=d(pk);_ngZone=d(B);_formField=d(ld,{optional:!0});_renderer=d(we);_uid=d(Te).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(bj,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new S;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=kt(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(zn.required)??!1}set required(e){this._required=kt(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Tb().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=kt(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Tb().has(e));constructor(){let e=d(Rr,{optional:!0}),i=d(Nr,{optional:!0}),r=d(xo),o=d(gk,{optional:!0,self:!0}),s=d(fh,{optional:!0,self:!0}),a=this._elementRef.nativeElement,c=a.nodeName.toLowerCase();o?Dn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ja(r,s||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=c==="select",this._isTextarea=c==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Kn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){vj.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&A("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(ht("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),G("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),H("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",L]},exportAs:["matInput"],features:[Oe([{provide:cd,useExisting:t}]),$e]})}return t})();function yj(t,n){if(t&1){let e=lt();f(0,"section",0)(1,"div",1)(2,"div",2)(3,"div",3),M(4,"img",4),h(),f(5,"div",5)(6,"h1",6),v(7),h(),f(8,"p"),v(9),h(),f(10,"div",7)(11,"p",8),v(12),xe(13,"currency"),h()(),f(14,"div",9)(15,"button",10),A("click",function(){Ee(e);let r=E();return Se(r.updateCart())}),f(16,"mat-icon"),v(17,"shopping_cart"),h(),v(18),h(),f(19,"mat-form-field",11)(20,"mat-label"),v(21,"Quantity"),h(),f(22,"input",12),Dr("ngModelChange",function(r){Ee(e);let o=E();return io(o.quantity,r)||(o.quantity=r),Se(r)}),h(),nn(),h()(),M(23,"mat-divider",13),f(24,"p",14),v(25),h()()()()()}if(t&2){let e=E();p(4),x("src",kn(e.product.pictureUrl),Xn),p(3),oe(" ",e.product.name," "),p(2),oe("You have ",e.quantityInCart," of this item in your cart"),p(3),oe(" ",Me(13,9,e.product.price)," "),p(3),x("disabled",e.quantity===e.quantityInCart),p(3),oe(" ",e.getButtonText()," "),p(4),xr("ngModel",e.quantity),rn(),p(3),oe(" ",e.product.description," ")}}var _h=class t{shopservice=d(lo);activatedRoute=d(yn);cartService=d(rt);product=null;cdr=d(ge);quantityInCart=0;quantity=1;ngOnInit(){this.loadProduct()}loadProduct(){let n=this.activatedRoute.snapshot.paramMap.get("id");n&&this.shopservice.getProductById(+n).subscribe({next:e=>{this.product=e,this.updateQuantityInBasket(),this.cdr.detectChanges()},error:e=>{console.error("Error fetching product details:",e)}})}updateCart(){if(this.product)if(this.quantity>this.quantityInCart){let n=this.quantity-this.quantityInCart;this.quantityInCart+=n,this.cartService.addItemsToCart(this.product,n)}else{let n=this.quantityInCart-this.quantity;this.quantityInCart-=n,this.cartService.removeItemFromCart(this.product.id,n)}}updateQuantityInBasket(){this.quantityInCart=this.cartService.cart()?.cartItems.find(n=>n.productId===this.product?.id)?.quantity||0,this.quantity=this.quantityInCart||1}getButtonText(){return this.quantityInCart>0?"Update Cart":"Add to Cart"}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-product-details"]],decls:1,vars:1,consts:[[1,"py-8"],[1,"max-w-screen-2xl","px-4","mx-auto"],[1,"grid","grid-cols-2","gap-8"],[1,"max-w-xl","mx-auto"],["alt","product image",1,"w-full",2,"width","500px","height","500px","object-fit","cover",3,"src"],[1,"mt-6","sm:mt-8","lg:mt-0"],[1,"text-2xl","font-semibold","text-gray-900"],[1,"mt-4","items-center","gap-4","flex"],[1,"text-3xl","font-extrabold","text-gray-900"],[1,"flex","gap-4","mt-6"],["matButton","outlined",1,"match-input-height",3,"click","disabled"],["appearance","outline",1,"flex","flex-row"],["matInput","","min","0","type","number","type","number",3,"ngModelChange","ngModel"],[1,"my-6"],[1,"mt-6","text-gray-500"]],template:function(e,i){e&1&&O(0,yj,26,11,"section",0),e&2&&P(i.product?0:-1)},dependencies:[it,ln,ci,Do,ai,Rs,Co,rr,ay,or,oy,As,St],encapsulation:2})};var vh=class t{item=vn.required();cartService=d(rt);incrementQuantity(){this.cartService.addItemsToCart(this.item())}decrementQuantity(){this.cartService.removeItemFromCart(this.item().productId,1)}removeItemFromCart(){this.cartService.removeItemFromCart(this.item().productId,this.item().quantity)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-cart-item"]],inputs:{item:[1,"item"]},decls:27,vars:11,consts:[[1,"rounded-lg","border","border-gray-200","bg-white","p-4","shadow-sm","mb-4"],[1,"flex","items-center","justify-between","gap-6"],[1,"shrink","order-1",3,"routerLink"],["alt","product image",1,"h-20","w-20",3,"src"],[1,"flex","items-center","justify-between","order-3"],[1,"flex","items-center","align-middle","gap-3"],["mat-icon-button",""],[1,"text-red-600",3,"click"],[1,"font-semibold","text-xl","mb-1"],["mat-icon-button","",3,"click"],[1,"text-green-600"],[1,"text-end","order-4","w-32"],[1,"font-bold","text-xl","text-gray-900"],[1,"w-full","flex","flex-col","flex-1","space-y-4","order-2","max-w-md"],[1,"font-medium",3,"routerLink"],[1,"flex","items-center","gap-4"],["mat-button","",1,"text-red-700","flex","gap-2","items-center",3,"click"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"a",2),M(3,"img",3),h(),f(4,"div",4)(5,"div",5)(6,"button",6)(7,"mat-icon",7),A("click",function(){return i.decrementQuantity()}),v(8,"remove"),h()(),f(9,"div",8),v(10),h(),f(11,"button",9),A("click",function(){return i.incrementQuantity()}),f(12,"mat-icon",10),v(13,"add"),h()()(),f(14,"div",11)(15,"p",12),v(16),xe(17,"currency"),h()()(),f(18,"div",13)(19,"a",14),v(20),h(),f(21,"div",15)(22,"button",16),A("click",function(){return i.removeItemFromCart()}),f(23,"mat-icon"),v(24,"delete"),h(),f(25,"span"),v(26,"Delete"),h()()()()()()),e&2&&(p(2),x("routerLink",Jn("/shop/",i.item().productId)),p(),x("src",kn(i.item().pictureUrl),Xn),p(7),oe(" ",i.item().quantity),p(6),X(Me(17,9,i.item().price)),p(3),x("routerLink",Jn("/shop/",i.item().productId)),p(),oe(" ",i.item().productName," "))},dependencies:[Nt,ln,St],encapsulation:2})};function Cj(t,n){t&1&&(f(0,"div",13)(1,"button",24),v(2,"Checkout"),h(),f(3,"button",25),v(4,"Continue Shopping"),h()())}var ec=class t{cartService=d(rt);router=d(Ke);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-order-summary"]],decls:47,vars:13,consts:[["form","ngForm"],[1,"mx-auto","max-w-4xl","flex-1","space-y-6","w-full"],[1,"space-y-4","rounded-lg","border","border-gray-200","bg-white","p-4","shadow-sm"],[1,"text-xl","font-semibold"],[1,"space-y-4"],[1,"space-y-2"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium","text-gray-500"],[1,"font-medium","text-gray-900"],[1,"font-medium","text-green-500"],[1,"flex","items-center","justify-between","gap-4","border-t","border-gray-200","pt-2"],[1,"font-bold","text-gray-900"],[1,"font-semibold","text-gray-900"],[1,"flex","flex-col","gap-2"],[1,"space-y-4","rounded-lg","border","border-gray-200","bg-white","shadow-sm"],[1,"space-y-2","flex","flex-col","p-2"],[1,"mb-2","block","text-sm","font-medium"],[1,"flex","justify-between","items-center"],[1,"text-sm","font-semibold"],["mat-icon-button",""],["color","warn"],["appearance","outline"],["name","code","type","text","matInput",""],["type","submit","matButton","outlined",1,"custom-shadow-btn"],["routerLink","/checkout","matButton","outlined",1,"custom-shadow-btn"],["routerLink","/shop","mat-button",""]],template:function(e,i){e&1&&(f(0,"div",1)(1,"div",2)(2,"p",3),v(3,"Order summary"),h(),f(4,"div",4)(5,"div",5)(6,"dl",6)(7,"dt",7),v(8,"Subtotal"),h(),f(9,"dd",8),v(10),xe(11,"currency"),h()(),f(12,"dl",6)(13,"dt",7),v(14,"Discount"),h(),f(15,"dd",9),v(16),xe(17,"currency"),h()(),f(18,"dl",6)(19,"dt",7),v(20,"Delivery fee"),h(),f(21,"dd",8),v(22),xe(23,"currency"),h()()(),f(24,"dl",10)(25,"dt",11),v(26,"Total"),h(),f(27,"dd",12),v(28),xe(29,"currency"),h()()(),O(30,Cj,5,0,"div",13),h(),f(31,"div",14)(32,"form",15,0)(34,"label",16),v(35,"Do you have a voucher code?"),h(),f(36,"div",17),M(37,"span",18),f(38,"button",19)(39,"mat-icon",20),v(40,"delete"),h()()(),f(41,"mat-form-field",21)(42,"mat-label"),v(43,"Voucher code"),h(),M(44,"input",22),h(),f(45,"button",23),v(46,"Apply code"),h()()()()),e&2&&(p(10),X(Me(11,5,i.cartService.totals()?.subtotal)),p(6),oe(" -",Me(17,7,i.cartService.totals()?.discount)),p(6),X(Me(23,9,i.cartService.totals()?.shipping)),p(6),X(Me(29,11,i.cartService.totals()?.total)),p(2),P(i.router.url!=="/checkout"?30:-1))},dependencies:[ci,ai,it,Nt,Do,Co,yo,bo,Rr,ln,St],encapsulation:2})};var bh=class t{message=vn.required();icon=vn.required();actionText=vn.required();action=km();onAction(){this.action.emit()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-empty-state"]],inputs:{message:[1,"message"],icon:[1,"icon"],actionText:[1,"actionText"]},outputs:{action:"action"},decls:8,vars:3,consts:[[1,"max-w-screen-xl","mx-auto","mt-32","px-10","py-4","bg-white","rounded-lg","shadow-md","w-full"],[1,"flex","flex-col","items-center","justify-center","py-12","w-full"],[1,"icon-display","mb-8"],[1,"text-gray-600","text-lg","font-semibold","mb-4"],["mat-flat-button","",3,"click"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"mat-icon",2),v(3),h(),f(4,"p",3),v(5),h(),f(6,"button",4),A("click",function(){return i.onAction()}),v(7),h()()()),e&2&&(p(3),X(i.icon()),p(2),oe(" ",i.message()," "),p(2),X(i.actionText()))},dependencies:[it,ln],encapsulation:2})};var wj=(t,n)=>n.productId;function xj(t,n){if(t&1&&M(0,"app-cart-item",3),t&2){let e=n.$implicit;x("item",e)}}function Dj(t,n){if(t&1&&(f(0,"div",0)(1,"div",2),at(2,xj,1,1,"app-cart-item",3,wj),h(),f(4,"div",4),M(5,"app-order-summary"),h()()),t&2){let e=E();p(2),ct(e.cartService.cart()?.cartItems)}}function Ej(t,n){if(t&1){let e=lt();f(0,"app-empty-state",5),A("action",function(){Ee(e);let r=E();return Se(r.onAction())}),h()}}var yh=class t{cartService=d(rt);router=d(Ke);onAction(){this.router.navigateByUrl("/shop")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-cart"]],decls:3,vars:1,consts:[[1,"flex","w-full","items-start","gap-6","mt-12"],["message","Your shopping cart is empty","icon","remove_shopping_cart","actionText","Go to shop"],[1,"w-3/4"],[3,"item"],[1,"w-1/4"],["message","Your shopping cart is empty","icon","remove_shopping_cart","actionText","Go to shop",3,"action"]],template:function(e,i){e&1&&(f(0,"section"),O(1,Dj,6,0,"div",0)(2,Ej,1,0,"app-empty-state",1),h()),e&2&&(p(),P(i.cartService.cart()?.cartItems?.length>0?1:2))},dependencies:[vh,ec,bh],encapsulation:2})};var Sj=["*"];function Ij(t,n){t&1&&ie(0)}var _y=(()=>{class t{_elementRef=d(F);focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkStepHeader",""]],hostAttrs:["role","tab"]})}return t})(),vy=(()=>{class t{template=d(gt);static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkStepLabel",""]]})}return t})();var Ns={NUMBER:"number",EDIT:"edit",DONE:"done",ERROR:"error"},kj=new y("STEPPER_GLOBAL_OPTIONS"),Ch=(()=>{class t{_stepperOptions;_stepper=d(tc);_displayDefaultIndicatorType;stepLabel;_childForms;content;stepControl;get interacted(){return this._interacted()}set interacted(e){this._interacted.set(e)}_interacted=V(!1);interactedStream=new U;label;errorMessage;ariaLabel;ariaLabelledby;get state(){return this._state()}set state(e){this._state.set(e)}_state=V(void 0);get editable(){return this._editable()}set editable(e){this._editable.set(e)}_editable=V(!0);optional=!1;get completed(){let e=this._completedOverride(),i=this._interacted();return e??(i&&(!this.stepControl||this.stepControl.valid))}set completed(e){this._completedOverride.set(e)}_completedOverride=V(null);index=V(-1);isSelected=dt(()=>this._stepper.selectedIndex===this.index());indicatorType=dt(()=>{let e=this.isSelected(),i=this.completed,r=this._state()??Ns.NUMBER,o=this._editable();return this._showError()&&this.hasError&&!e?Ns.ERROR:this._displayDefaultIndicatorType?!i||e?Ns.NUMBER:o?Ns.EDIT:Ns.DONE:i&&!e?Ns.DONE:i&&e?r:o&&e?Ns.EDIT:r});isNavigable=dt(()=>{let e=this.isSelected();return this.completed||e||!this._stepper.linear});get hasError(){let e=this._customError();return e??this._getDefaultError()}set hasError(e){this._customError.set(e)}_customError=V(null);_getDefaultError(){return this.interacted&&!!this.stepControl?.invalid}constructor(){let e=d(kj,{optional:!0});this._stepperOptions=e||{},this._displayDefaultIndicatorType=this._stepperOptions.displayDefaultIndicatorType!==!1}select(){this._stepper.selected=this}reset(){this._interacted.set(!1),this._completedOverride()!=null&&this._completedOverride.set(!1),this._customError()!=null&&this._customError.set(!1),this.stepControl&&(this._childForms?.forEach(e=>e.resetForm?.()),this.stepControl.reset())}ngOnChanges(){this._stepper._stateChanged()}_markAsInteracted(){this._interacted()||(this._interacted.set(!0),this.interactedStream.emit(this))}_showError(){return this._stepperOptions.showError??this._customError()!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["cdk-step"]],contentQueries:function(i,r,o){if(i&1&&yt(o,vy,5)(o,Ii,5),i&2){let s;z(s=$())&&(r.stepLabel=s.first),z(s=$())&&(r._childForms=s)}},viewQuery:function(i,r){if(i&1&&je(gt,7),i&2){let o;z(o=$())&&(r.content=o.first)}},inputs:{stepControl:"stepControl",label:"label",errorMessage:"errorMessage",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],state:"state",editable:[2,"editable","editable",L],optional:[2,"optional","optional",L],completed:[2,"completed","completed",L],hasError:[2,"hasError","hasError",L]},outputs:{interactedStream:"interacted"},exportAs:["cdkStep"],features:[$e],ngContentSelectors:Sj,decls:1,vars:0,template:function(i,r){i&1&&(ke(),ha(0,Ij,1,0,"ng-template"))},encapsulation:2})}return t})(),tc=(()=>{class t{_dir=d(Zt,{optional:!0});_changeDetectorRef=d(ge);_elementRef=d(F);_destroyed=new S;_keyManager;_steps;steps=new En;_stepHeader;_sortedHeaders=new En;get linear(){return this._linear()}set linear(e){this._linear.set(e)}_linear=V(!1);get selectedIndex(){return this._selectedIndex()}set selectedIndex(e){this._steps?(this._isValidIndex(e),this.selectedIndex!==e&&(this.selected?._markAsInteracted(),!this._anyControlsInvalidOrPending(e)&&(e>=this.selectedIndex||this.steps.toArray()[e].editable)&&this._updateSelectedItemIndex(e))):this._selectedIndex.set(e)}_selectedIndex=V(0);get selected(){return this.steps?this.steps.toArray()[this.selectedIndex]:void 0}set selected(e){this.selectedIndex=e&&this.steps?this.steps.toArray().indexOf(e):-1}selectionChange=new U;selectedIndexChange=new U;_groupId=d(Te).getId("cdk-stepper-");get orientation(){return this._orientation}set orientation(e){this._orientation=e,this._keyManager&&this._keyManager.withVerticalOrientation(e==="vertical")}_orientation="horizontal";ngAfterContentInit(){this._steps.changes.pipe(et(this._steps),pe(this._destroyed)).subscribe(e=>{this.steps.reset(e.filter(i=>i._stepper===this)),this.steps.forEach((i,r)=>i.index.set(r)),this.steps.notifyOnChanges()})}ngAfterViewInit(){if(this._stepHeader.changes.pipe(et(this._stepHeader),pe(this._destroyed)).subscribe(e=>{this._sortedHeaders.reset(e.toArray().sort((i,r)=>i._elementRef.nativeElement.compareDocumentPosition(r._elementRef.nativeElement)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1)),this._sortedHeaders.notifyOnChanges()}),this._keyManager=new Mr(this._sortedHeaders).withWrap().withHomeAndEnd().withVerticalOrientation(this._orientation==="vertical"),this._keyManager.updateActiveItem(this.selectedIndex),(this._dir?this._dir.change:W()).pipe(et(this._layoutDirection()),pe(this._destroyed)).subscribe(e=>this._keyManager?.withHorizontalOrientation(e)),this._keyManager.updateActiveItem(this.selectedIndex),this.steps.changes.subscribe(()=>{this.selected||this._selectedIndex.set(Math.max(this.selectedIndex-1,0))}),this._isValidIndex(this.selectedIndex)||this._selectedIndex.set(0),this.linear&&this.selectedIndex>0){let e=this.steps.toArray().slice(0,this._selectedIndex());for(let i of e)i._markAsInteracted()}}ngOnDestroy(){this._keyManager?.destroy(),this.steps.destroy(),this._sortedHeaders.destroy(),this._destroyed.next(),this._destroyed.complete()}next(){this.selectedIndex=Math.min(this._selectedIndex()+1,this.steps.length-1)}previous(){this.selectedIndex=Math.max(this._selectedIndex()-1,0)}reset(){this._updateSelectedItemIndex(0),this.steps.forEach(e=>e.reset()),this._stateChanged()}_getStepLabelId(e){return`${this._groupId}-label-${e}`}_getStepContentId(e){return`${this._groupId}-content-${e}`}_stateChanged(){this._changeDetectorRef.markForCheck()}_getAnimationDirection(e){let i=e-this._selectedIndex();return i<0?this._layoutDirection()==="rtl"?"next":"previous":i>0?this._layoutDirection()==="rtl"?"previous":"next":"current"}_getFocusIndex(){return this._keyManager?this._keyManager.activeItemIndex:this._selectedIndex()}_updateSelectedItemIndex(e){let i=this.steps.toArray(),r=this._selectedIndex();this.selectionChange.emit({selectedIndex:e,previouslySelectedIndex:r,selectedStep:i[e],previouslySelectedStep:i[r]}),this._keyManager&&(this._containsFocus()?this._keyManager.setActiveItem(e):this._keyManager.updateActiveItem(e)),this._selectedIndex.set(e),this.selectedIndexChange.emit(e),this._stateChanged()}_onKeydown(e){let i=Ct(e),r=e.keyCode,o=this._keyManager;o?.activeItemIndex!=null&&!i&&(r===32||r===13)?(this.selectedIndex=o.activeItemIndex,e.preventDefault()):o?.setFocusOrigin("keyboard").onKeydown(e)}_anyControlsInvalidOrPending(e){return this.linear&&e>=0?this.steps.toArray().slice(0,e).some(i=>{let r=i.stepControl;return(r?r.invalid||r.pending||!i.interacted:!i.completed)&&!i.optional&&!i._completedOverride()}):!1}_layoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_containsFocus(){let e=this._elementRef.nativeElement,i=Xi();return e===i||e.contains(i)}_isValidIndex(e){return e>-1&&(!this.steps||e<this.steps.length)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkStepper",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,Ch,5)(o,_y,5),i&2){let s;z(s=$())&&(r._steps=s),z(s=$())&&(r._stepHeader=s)}},inputs:{linear:[2,"linear","linear",L],selectedIndex:[2,"selectedIndex","selectedIndex",Et],selected:"selected",orientation:"orientation"},outputs:{selectionChange:"selectionChange",selectedIndexChange:"selectedIndexChange"},exportAs:["cdkStepper"]})}return t})(),_k=(()=>{class t{_stepper=d(tc);type="submit";static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["button","cdkStepperNext",""]],hostVars:1,hostBindings:function(i,r){i&1&&A("click",function(){return r._stepper.next()}),i&2&&ht("type",r.type)},inputs:{type:"type"}})}return t})(),vk=(()=>{class t{_stepper=d(tc);type="button";static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["button","cdkStepperPrevious",""]],hostVars:1,hostBindings:function(i,r){i&1&&A("click",function(){return r._stepper.previous()}),i&2&&ht("type",r.type)},inputs:{type:"type"}})}return t})(),bk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Wt]})}return t})();var Mj=(t,n,e)=>({index:t,active:n,optional:e});function Tj(t,n){if(t&1&&on(0,2),t&2){let e=E();x("ngTemplateOutlet",e.iconOverrides[e.state])("ngTemplateOutletContext",uv(2,Mj,e.index,e.active,e.optional))}}function Aj(t,n){if(t&1&&(f(0,"span",7),v(1),h()),t&2){let e=E(2);p(),X(e._getDefaultTextForState(e.state))}}function Rj(t,n){if(t&1&&(f(0,"span",8),v(1),h()),t&2){let e=E(3);p(),X(e._intl.completedLabel)}}function Nj(t,n){if(t&1&&(f(0,"span",8),v(1),h()),t&2){let e=E(3);p(),X(e._intl.editableLabel)}}function Oj(t,n){if(t&1&&(O(0,Rj,2,1,"span",8)(1,Nj,2,1,"span",8),f(2,"mat-icon",7),v(3),h()),t&2){let e=E(2);P(e.state==="done"?0:e.state==="edit"?1:-1),p(3),X(e._getDefaultTextForState(e.state))}}function Pj(t,n){if(t&1&&O(0,Aj,2,1,"span",7)(1,Oj,4,2),t&2){let e,i=E();P((e=i.state)==="number"?0:1)}}function Fj(t,n){t&1&&(f(0,"div",4),on(1,9),h()),t&2&&(p(),x("ngTemplateOutlet",n.template))}function Lj(t,n){if(t&1&&(f(0,"div",4),v(1),h()),t&2){let e=E();p(),X(e.label)}}function Bj(t,n){if(t&1&&(f(0,"div",5),v(1),h()),t&2){let e=E();p(),X(e._intl.optionalLabel)}}function Vj(t,n){if(t&1&&(f(0,"div",6),v(1),h()),t&2){let e=E();p(),X(e.errorMessage)}}var yk=["*"];function jj(t,n){}function Hj(t,n){if(t&1&&(ie(0),Ge(1,jj,0,0,"ng-template",0)),t&2){let e=E();p(),x("cdkPortalOutlet",e._portal)}}var Uj=["animatedContainer"],Ck=t=>({steps:t}),wk=t=>({step:t});function zj(t,n){t&1&&ie(0)}function $j(t,n){if(t&1&&(f(0,"div",5),on(1,9)(2,6),h()),t&2){let e=E(2),i=He(6);p(),x("ngTemplateOutlet",e.headerPrefix()),p(),x("ngTemplateOutlet",i)("ngTemplateOutletContext",ga(3,Ck,e.steps))}}function Gj(t,n){if(t&1&&on(0,6),t&2){let e=E(2),i=He(6);x("ngTemplateOutlet",i)("ngTemplateOutletContext",ga(2,Ck,e.steps))}}function Wj(t,n){if(t&1&&(f(0,"div",10,2),on(2,9),h()),t&2){let e=n.$implicit,i=n.$index,r=E(2);jt("mat-horizontal-stepper-content-"+r._getAnimationDirection(i)),x("id",r._getStepContentId(i)),G("aria-labelledby",r._getStepLabelId(i))("inert",r.selectedIndex===i?null:""),p(2),x("ngTemplateOutlet",e.content)}}function qj(t,n){if(t&1&&(f(0,"div",3),O(1,$j,3,5,"div",5)(2,Gj,1,4,"ng-container",6),f(3,"div",7),at(4,Wj,3,6,"div",8,Cr),h()()),t&2){let e=E();p(),P(e.headerPrefix()?1:2),p(3),ct(e.steps)}}function Yj(t,n){if(t&1&&on(0,9),t&2){let e=E(2);x("ngTemplateOutlet",e.headerPrefix())}}function Zj(t,n){if(t&1&&(f(0,"div",11),on(1,6),f(2,"div",12,2)(4,"div",13)(5,"div",14),on(6,9),h()()()()),t&2){let e=n.$implicit,i=n.$index,r=n.$index,o=n.$count,s=E(2),a=He(4);p(),x("ngTemplateOutlet",a)("ngTemplateOutletContext",ga(11,wk,e)),p(),H("mat-stepper-vertical-line",r!==o-1)("mat-vertical-content-container-active",s.selectedIndex===i),G("inert",s.selectedIndex===i?null:"")("aria-label",s.ariaLabel),p(2),x("id",s._getStepContentId(i)),G("aria-labelledby",s._getStepLabelId(i)),p(2),x("ngTemplateOutlet",e.content)}}function Qj(t,n){if(t&1&&(f(0,"div",4),O(1,Yj,1,1,"ng-container",9),at(2,Zj,7,13,"div",11,Cr),h()),t&2){let e=E();p(),P(e.headerPrefix()?1:-1),p(),ct(e.steps)}}function Kj(t,n){if(t&1){let e=lt();f(0,"mat-step-header",15),A("click",function(){let r=Ee(e).step;return Se(r.select())})("keydown",function(r){Ee(e);let o=E();return Se(o._onKeydown(r))}),h()}if(t&2){let e=n.step,i=E();H("mat-horizontal-stepper-header",i.orientation==="horizontal")("mat-vertical-stepper-header",i.orientation==="vertical"),x("tabIndex",i._getFocusIndex()===e.index()?0:-1)("id",i._getStepLabelId(e.index()))("index",e.index())("state",e.indicatorType())("label",e.stepLabel||e.label)("selected",e.isSelected())("active",e.isNavigable())("optional",e.optional)("errorMessage",e.errorMessage)("iconOverrides",i._iconOverrides)("disableRipple",i.disableRipple||!e.isNavigable())("color",e.color||i.color),G("role",i.orientation==="horizontal"?"tab":"button")("aria-posinset",i.orientation==="horizontal"?e.index()+1:null)("aria-setsize",i.orientation==="horizontal"?i.steps.length:null)("aria-selected",i.orientation==="horizontal"?e.isSelected():null)("aria-current",i.orientation==="vertical"&&e.isSelected()?"step":null)("aria-disabled",i.orientation==="vertical"&&e.isSelected()?"true":null)("aria-expanded",i.orientation==="vertical"?e.isSelected():null)("aria-controls",i._getStepContentId(e.index()))("aria-label",e.ariaLabel||null)("aria-labelledby",!e.ariaLabel&&e.ariaLabelledby?e.ariaLabelledby:null)("aria-disabled",e.isNavigable()?null:!0)}}function Xj(t,n){t&1&&M(0,"div",17)}function Jj(t,n){if(t&1&&(on(0,6),O(1,Xj,1,0,"div",17)),t&2){let e=n.$implicit,i=n.$index,r=n.$count;E(2);let o=He(4);x("ngTemplateOutlet",o)("ngTemplateOutletContext",ga(3,wk,e)),p(),P(i!==r-1?1:-1)}}function eH(t,n){if(t&1&&(f(0,"div",16),at(1,Jj,2,5,null,null,Cr),h()),t&2){let e=n.steps,i=E();G("aria-label",i.ariaLabel),p(),ct(e)}}var by=(()=>{class t extends vy{static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","matStepLabel",""]],features:[be]})}return t})(),tH=(()=>{class t{changes=new S;optionalLabel="Optional";completedLabel="Completed";editableLabel="Editable";static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),yy=(()=>{class t extends _y{_intl=d(tH);_focusMonitor=d(An);_intlSubscription;state;label;errorMessage;iconOverrides;index;selected=!1;active=!1;optional=!1;disableRipple=!1;color;constructor(){super();let e=d(qe);e.load(Rn),e.load(Ji);let i=d(ge);this._intlSubscription=this._intl.changes.subscribe(()=>i.markForCheck())}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){this._intlSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._elementRef)}focus(e,i){e?this._focusMonitor.focusVia(this._elementRef,e,i):this._elementRef.nativeElement.focus(i)}_stringLabel(){return this.label instanceof by?null:this.label}_templateLabel(){return this.label instanceof by?this.label:null}_getHostElement(){return this._elementRef.nativeElement}_getDefaultTextForState(e){return e=="number"?`${this.index+1}`:e=="edit"?"create":e=="error"?"warning":e}_hasEmptyLabel(){return!this._stringLabel()&&!this._templateLabel()&&!this._hasOptionalLabel()&&!this._hasErrorLabel()}_hasOptionalLabel(){return this.optional&&this.state!=="error"}_hasErrorLabel(){return this.state==="error"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-step-header"]],hostAttrs:["role","",1,"mat-step-header"],hostVars:4,hostBindings:function(i,r){i&2&&(jt("mat-"+(r.color||"primary")),H("mat-step-header-empty-label",r._hasEmptyLabel()))},inputs:{state:"state",label:"label",errorMessage:"errorMessage",iconOverrides:"iconOverrides",index:"index",selected:"selected",active:"active",optional:"optional",disableRipple:"disableRipple",color:"color"},features:[be],decls:10,vars:17,consts:[["matRipple","",1,"mat-step-header-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"],[1,"mat-step-icon-content"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-step-label"],[1,"mat-step-text-label"],[1,"mat-step-optional"],[1,"mat-step-sub-label-error"],["aria-hidden","true"],[1,"cdk-visually-hidden"],[3,"ngTemplateOutlet"]],template:function(i,r){if(i&1&&(M(0,"div",0),f(1,"div")(2,"div",1),O(3,Tj,1,6,"ng-container",2)(4,Pj,2,1),h()(),f(5,"div",3),O(6,Fj,2,1,"div",4)(7,Lj,2,1,"div",4),O(8,Bj,2,1,"div",5),O(9,Vj,2,1,"div",6),h()),i&2){let o;x("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disableRipple),p(),jt(Jn("mat-step-icon-state-",r.state," mat-step-icon")),H("mat-step-icon-selected",r.selected),p(2),P(r.iconOverrides&&r.iconOverrides[r.state]?3:4),p(2),H("mat-step-label-active",r.active)("mat-step-label-selected",r.selected)("mat-step-label-error",r.state=="error"),p(),P((o=r._templateLabel())?6:r._stringLabel()?7:-1,o),p(2),P(r._hasOptionalLabel()?8:-1),p(),P(r._hasErrorLabel()?9:-1)}},dependencies:[er,Qi,ln],styles:[`.mat-step-header {
  overflow: hidden;
  outline: none;
  cursor: pointer;
  position: relative;
  box-sizing: content-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-step-header:focus-visible .mat-focus-indicator::before {
  content: "";
}
.mat-step-header:hover[aria-disabled=true] {
  cursor: default;
}
.mat-step-header:hover:not([aria-disabled]), .mat-step-header:hover[aria-disabled=false] {
  background-color: var(--mat-stepper-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-hover-state-layer-shape, var(--mat-sys-corner-medium));
}
.mat-step-header:hover:not([aria-disabled]) .mat-step-header-ripple::before, .mat-step-header:hover[aria-disabled=false] .mat-step-header-ripple::before {
  border-radius: var(--mat-stepper-header-hover-state-layer-shape, var(--mat-sys-corner-medium));
}
.mat-step-header.cdk-keyboard-focused, .mat-step-header.cdk-program-focused {
  background-color: var(--mat-stepper-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-focus-state-layer-shape, var(--mat-sys-corner-medium));
}
.mat-step-header.cdk-keyboard-focused .mat-step-header-ripple::before, .mat-step-header.cdk-program-focused .mat-step-header-ripple::before {
  border-radius: var(--mat-stepper-header-focus-state-layer-shape, var(--mat-sys-corner-medium));
}
@media (hover: none) {
  .mat-step-header:hover {
    background: none;
  }
}
@media (forced-colors: active) {
  .mat-step-header {
    outline: solid 1px;
  }
  .mat-step-header[aria-selected=true] .mat-step-label {
    text-decoration: underline;
  }
  .mat-step-header[aria-disabled=true] {
    outline-color: GrayText;
  }
  .mat-step-header[aria-disabled=true] .mat-step-label,
  .mat-step-header[aria-disabled=true] .mat-step-icon,
  .mat-step-header[aria-disabled=true] .mat-step-optional {
    color: GrayText;
  }
}

.mat-step-optional {
  font-size: 12px;
  color: var(--mat-stepper-header-optional-label-text-color, var(--mat-sys-on-surface-variant));
}

.mat-step-sub-label-error {
  font-size: 12px;
  font-weight: normal;
}

.mat-step-icon {
  border-radius: 50%;
  height: 24px;
  width: 24px;
  flex-shrink: 0;
  position: relative;
  color: var(--mat-stepper-header-icon-foreground-color, var(--mat-sys-surface));
  background-color: var(--mat-stepper-header-icon-background-color, var(--mat-sys-on-surface-variant));
}

.mat-step-icon-content {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
}

.mat-step-icon .mat-icon {
  font-size: 16px;
  height: 16px;
  width: 16px;
}

.mat-step-icon-state-error {
  background-color: var(--mat-stepper-header-error-state-icon-background-color, transparent);
  color: var(--mat-stepper-header-error-state-icon-foreground-color, var(--mat-sys-error));
}
.mat-step-icon-state-error .mat-icon {
  font-size: 24px;
  height: 24px;
  width: 24px;
}

.mat-step-label {
  display: inline-block;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  min-width: 50px;
  vertical-align: middle;
  font-family: var(--mat-stepper-header-label-text-font, var(--mat-sys-title-small-font));
  font-size: var(--mat-stepper-header-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-label-text-weight, var(--mat-sys-title-small-weight));
  color: var(--mat-stepper-header-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-active {
  color: var(--mat-stepper-header-selected-state-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-error {
  color: var(--mat-stepper-header-error-state-label-text-color, var(--mat-sys-error));
  font-size: var(--mat-stepper-header-error-state-label-text-size, var(--mat-sys-title-small-size));
}
.mat-step-label.mat-step-label-selected {
  font-size: var(--mat-stepper-header-selected-state-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-selected-state-label-text-weight, var(--mat-sys-title-small-weight));
}
.mat-step-header-empty-label .mat-step-label {
  min-width: 0;
}

.mat-step-text-label {
  text-overflow: ellipsis;
  overflow: hidden;
}

.mat-step-header .mat-step-header-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-step-icon-selected {
  background-color: var(--mat-stepper-header-selected-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-selected-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-done {
  background-color: var(--mat-stepper-header-done-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-done-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-edit {
  background-color: var(--mat-stepper-header-edit-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-edit-state-icon-foreground-color, var(--mat-sys-on-primary));
}
`],encapsulation:2})}return t})(),nH=(()=>{class t{templateRef=d(gt);name;static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["ng-template","matStepperIcon",""]],inputs:{name:[0,"matStepperIcon","name"]}})}return t})(),iH=(()=>{class t{_template=d(gt);static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["ng-template","matStepContent",""]]})}return t})(),Cy=(()=>{class t extends Ch{_errorStateMatcher=d(xo,{skipSelf:!0});_viewContainerRef=d(Vt);_isSelected=he.EMPTY;stepLabel=void 0;color;_lazyContent;_portal;ngAfterContentInit(){this._isSelected=this._stepper.steps.changes.pipe(pt(()=>this._stepper.selectionChange.pipe(Y(e=>e.selectedStep===this),et(this._stepper.selected===this)))).subscribe(e=>{e&&this._lazyContent&&!this._portal&&(this._portal=new Un(this._lazyContent._template,this._viewContainerRef))})}ngOnDestroy(){this._isSelected.unsubscribe()}isErrorState(e,i){let r=this._errorStateMatcher.isErrorState(e,i),o=!!(e&&e.invalid&&this.interacted);return r||o}isSignalErrorState(e){let i=this._errorStateMatcher.isSignalErrorState?.(e)??!1,r=!!(e&&e().invalid()&&this.interacted);return i||r}static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275cmp=D({type:t,selectors:[["mat-step"]],contentQueries:function(i,r,o){if(i&1&&yt(o,by,5)(o,iH,5),i&2){let s;z(s=$())&&(r.stepLabel=s.first),z(s=$())&&(r._lazyContent=s.first)}},hostAttrs:["hidden",""],inputs:{color:"color"},exportAs:["matStep"],features:[Oe([{provide:xo,useExisting:t},{provide:Ch,useExisting:t}]),be],ngContentSelectors:yk,decls:1,vars:0,consts:[[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(ke(),Ge(0,Hj,2,1,"ng-template"))},dependencies:[tr],encapsulation:2})}return t})(),wy=(()=>{class t extends tc{_ngZone=d(B);_renderer=d(we);_animationsDisabled=Ae();_cleanupTransition;_isAnimating=V(!1);_stepHeader=void 0;_animatedContainers;_steps=void 0;steps=new En;_icons;animationDone=new U;disableRipple=!1;color;labelPosition="end";headerPosition="top";ariaLabel=null;headerPrefix=vn(null);_iconOverrides={};get animationDuration(){return this._animationDuration}set animationDuration(e){/^[0-9]+(?:\.[0-9]+)?$/.test(e)?this._animationDuration=e+"ms":/^[0-9]+(?:\.[0-9]+)?(?:ms|s)$/.test(e)?this._animationDuration=e:this._animationDuration=""}_animationDuration="";_isServer=!d(De).isBrowser;constructor(){super();let i=d(F).nativeElement.nodeName.toLowerCase();this.orientation=i==="mat-vertical-stepper"?"vertical":"horizontal"}ngAfterContentInit(){super.ngAfterContentInit(),this._icons.forEach(({name:e,templateRef:i})=>this._iconOverrides[e]=i),this.steps.changes.pipe(pe(this._destroyed)).subscribe(()=>this._stateChanged()),this.selectedIndexChange.pipe(pe(this._destroyed)).subscribe(()=>{let e=this._getAnimationDuration();e==="0ms"||e==="0s"?this._onAnimationDone():this._isAnimating.set(!0)}),this._ngZone.runOutsideAngular(()=>{this._animationsDisabled||setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-stepper-animations-enabled"),this._cleanupTransition=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionend)},200)})}ngAfterViewInit(){if(super.ngAfterViewInit(),typeof queueMicrotask=="function"){let e=!1;this._animatedContainers.changes.pipe(et(null),pe(this._destroyed)).subscribe(()=>queueMicrotask(()=>{e||(e=!0,this.animationDone.emit()),this._stateChanged()}))}}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransition?.()}_getAnimationDuration(){return this._animationsDisabled?"0ms":this.animationDuration?this.animationDuration:this.orientation==="horizontal"?"500ms":"225ms"}_handleTransitionend=e=>{let i=e.target;if(!i)return;let r=this.orientation==="horizontal"&&e.propertyName==="transform"&&i.classList.contains("mat-horizontal-stepper-content-current"),o=this.orientation==="vertical"&&e.propertyName==="grid-template-rows"&&i.classList.contains("mat-vertical-content-container-active");(r||o)&&this._animatedContainers.find(a=>a.nativeElement===i)&&this._onAnimationDone()};_onAnimationDone(){this._isAnimating.set(!1),this.animationDone.emit()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-stepper"],["mat-vertical-stepper"],["mat-horizontal-stepper"],["","matStepper",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,Cy,5)(o,nH,5),i&2){let s;z(s=$())&&(r._steps=s),z(s=$())&&(r._icons=s)}},viewQuery:function(i,r){if(i&1&&je(yy,5)(Uj,5),i&2){let o;z(o=$())&&(r._stepHeader=o),z(o=$())&&(r._animatedContainers=o)}},hostVars:14,hostBindings:function(i,r){i&2&&(wr("--mat-stepper-animation-duration",r._getAnimationDuration()),H("mat-stepper-horizontal",r.orientation==="horizontal")("mat-stepper-vertical",r.orientation==="vertical")("mat-stepper-label-position-end",r.orientation==="horizontal"&&r.labelPosition=="end")("mat-stepper-label-position-bottom",r.orientation==="horizontal"&&r.labelPosition=="bottom")("mat-stepper-header-position-bottom",r.headerPosition==="bottom")("mat-stepper-animating",r._isAnimating()))},inputs:{disableRipple:"disableRipple",color:"color",labelPosition:"labelPosition",headerPosition:"headerPosition",ariaLabel:[0,"aria-label","ariaLabel"],headerPrefix:[1,"headerPrefix"],animationDuration:"animationDuration"},outputs:{animationDone:"animationDone"},exportAs:["matStepper","matVerticalStepper","matHorizontalStepper"],features:[Oe([{provide:tc,useExisting:t}]),be],ngContentSelectors:yk,decls:7,vars:2,consts:[["stepTemplate",""],["horizontalStepsTemplate",""],["animatedContainer",""],[1,"mat-horizontal-stepper-wrapper"],[1,"mat-vertical-stepper-wrapper"],[1,"mat-horizontal-stepper-header-wrapper"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-horizontal-content-container"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id","class"],[3,"ngTemplateOutlet"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id"],[1,"mat-step"],[1,"mat-vertical-content-container"],["role","region",1,"mat-vertical-stepper-content",3,"id"],[1,"mat-vertical-content"],[3,"click","keydown","tabIndex","id","index","state","label","selected","active","optional","errorMessage","iconOverrides","disableRipple","color"],["aria-orientation","horizontal","role","tablist",1,"mat-horizontal-stepper-header-container"],[1,"mat-stepper-horizontal-line"]],template:function(i,r){if(i&1&&(ke(),O(0,zj,1,0),O(1,qj,6,1,"div",3)(2,Qj,4,1,"div",4),Ge(3,Kj,1,27,"ng-template",null,0,ei)(5,eH,3,1,"ng-template",null,1,ei)),i&2){let o;P(r._isServer?0:-1),p(),P((o=r.orientation)==="horizontal"?1:o==="vertical"?2:-1)}},dependencies:[Qi,yy],styles:[`.mat-stepper-vertical,
.mat-stepper-horizontal {
  display: block;
  font-family: var(--mat-stepper-container-text-font, var(--mat-sys-body-medium-font));
  background: var(--mat-stepper-container-color, var(--mat-sys-surface));
}

.mat-horizontal-stepper-header-wrapper {
  align-items: center;
  display: flex;
}

.mat-horizontal-stepper-header-container {
  white-space: nowrap;
  display: flex;
  align-items: center;
  flex-grow: 1;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header-container {
  align-items: flex-start;
}
.mat-stepper-header-position-bottom .mat-horizontal-stepper-header-container {
  order: 1;
}

.mat-stepper-horizontal-line {
  border-top-width: 1px;
  border-top-style: solid;
  flex: auto;
  height: 0;
  margin: 0 -16px;
  min-width: 32px;
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-stepper-horizontal-line {
  margin: 0;
  min-width: 0;
  position: relative;
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}

.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  border-top-width: 1px;
  border-top-style: solid;
  content: "";
  display: inline-block;
  height: 0;
  position: absolute;
  width: calc(50% - 20px);
}

.mat-horizontal-stepper-header {
  display: flex;
  overflow: hidden;
  align-items: center;
  padding: 0 24px;
  height: var(--mat-stepper-header-height, 72px);
}
.mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 8px;
  flex: none;
}
[dir=rtl] .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 8px;
}
.mat-horizontal-stepper-header.mat-step-header-empty-label .mat-step-icon {
  margin: 0;
}
.mat-horizontal-stepper-header::before, .mat-horizontal-stepper-header::after {
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header::after {
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  box-sizing: border-box;
  flex-direction: column;
  height: auto;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  right: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before {
  left: 0;
}
[dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:last-child::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:first-child::after {
  display: none;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-label {
  padding: 16px 0 0 0;
  text-align: center;
  width: 100%;
}

.mat-vertical-stepper-header {
  display: flex;
  align-items: center;
  height: 24px;
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-vertical-stepper-header .mat-step-icon {
  margin-right: 12px;
}
[dir=rtl] .mat-vertical-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 12px;
}

.mat-horizontal-stepper-wrapper {
  display: flex;
  flex-direction: column;
}

.mat-horizontal-stepper-content {
  visibility: hidden;
  overflow: hidden;
  outline: 0;
  height: 0;
}
.mat-stepper-animations-enabled .mat-horizontal-stepper-content {
  transition: transform var(--mat-stepper-animation-duration, 0) cubic-bezier(0.35, 0, 0.25, 1);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-previous {
  transform: translate3d(-100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-next {
  transform: translate3d(100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  visibility: visible;
  transform: none;
  height: auto;
}
.mat-stepper-horizontal:not(.mat-stepper-animating) .mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  overflow: visible;
}

.mat-horizontal-content-container {
  overflow: hidden;
  padding: 0 24px 24px 24px;
}
@media (forced-colors: active) {
  .mat-horizontal-content-container {
    outline: solid 1px;
  }
}
.mat-stepper-header-position-bottom .mat-horizontal-content-container {
  padding: 24px 24px 0 24px;
}

.mat-vertical-content-container {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
  margin-left: 36px;
  border: 0;
  position: relative;
}
.mat-stepper-animations-enabled .mat-vertical-content-container {
  transition: grid-template-rows var(--mat-stepper-animation-duration, 0) cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-vertical-content-container.mat-vertical-content-container-active {
  grid-template-rows: 1fr;
}
.mat-step:last-child .mat-vertical-content-container {
  border: none;
}
@media (forced-colors: active) {
  .mat-vertical-content-container {
    outline: solid 1px;
  }
}
[dir=rtl] .mat-vertical-content-container {
  margin-left: 0;
  margin-right: 36px;
}
@supports not (grid-template-rows: 0fr) {
  .mat-vertical-content-container {
    height: 0;
  }
  .mat-vertical-content-container.mat-vertical-content-container-active {
    height: auto;
  }
}

.mat-stepper-vertical-line::before {
  content: "";
  position: absolute;
  left: 0;
  border-left-width: 1px;
  border-left-style: solid;
  border-left-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
  top: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
  bottom: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
}
[dir=rtl] .mat-stepper-vertical-line::before {
  left: auto;
  right: 0;
}

.mat-vertical-stepper-content {
  overflow: hidden;
  outline: 0;
  visibility: hidden;
}
.mat-stepper-animations-enabled .mat-vertical-stepper-content {
  transition: visibility var(--mat-stepper-animation-duration, 0) linear;
}
.mat-vertical-content-container-active > .mat-vertical-stepper-content {
  visibility: visible;
}

.mat-vertical-content {
  padding: 0 24px 24px 24px;
}
`],encapsulation:2})}return t})(),xk=(()=>{class t extends _k{static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["button","matStepperNext",""]],hostAttrs:[1,"mat-stepper-next"],hostVars:1,hostBindings:function(i,r){i&2&&ht("type",r.type)},features:[be]})}return t})(),Dk=(()=>{class t extends vk{static \u0275fac=(()=>{let e;return function(r){return(e||(e=_t(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["button","matStepperPrevious",""]],hostAttrs:[1,"mat-stepper-previous"],hostVars:1,hostBindings:function(i,r){i&2&&ht("type",r.type)},features:[be]})}return t})(),Ek=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({providers:[xo],imports:[za,bk,Nf,Ha,wy,yy,Wt]})}return t})();function oH(t,n){if(t&1){let e=lt();f(0,"div",1)(1,"button",2),A("click",function(){Ee(e);let r=E();return Se(r.action())}),v(2),h()()}if(t&2){let e=E();p(2),oe(" ",e.data.action," ")}}var sH=["label"];function aH(t,n){}var cH=Math.pow(2,31)-1,dd=class{_overlayRef;instance;containerInstance;_afterDismissed=new S;_afterOpened=new S;_onAction=new S;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,cH))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Sk=new y("MatSnackBarData"),nc=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},lH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),dH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),uH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),mH=(()=>{class t{snackBarRef=d(dd);data=d(Sk);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(f(0,"div",0),v(1),h(),O(2,oH,3,1,"div",1)),i&2&&(p(),oe(" ",r.data.message,`
`),p(),P(r.hasAction?2:-1))},dependencies:[it,lH,dH,uH],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})}return t})(),xy="_mat-snack-bar-enter",Dy="_mat-snack-bar-exit",fH=(()=>{class t extends fo{_ngZone=d(B);_elementRef=d(F);_changeDetectorRef=d(ge);_platform=d(De);_animationsDisabled=Ae();snackBarConfig=d(nc);_document=d(Z);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(q);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new S;_onExit=new S;_onEnter=new S;_animationState="void";_live;_label;_role;_liveElementId=d(Te).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===Dy?this._completeExit():e===xy&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?vt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(xy)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(xy)},200)))}exit(){return this._destroyed?W(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?vt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Dy)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Dy),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&je(tr,7)(sH,7),i&2){let o;z(o=$())&&(r._portalOutlet=o.first),z(o=$())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&A("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&H("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[be],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2,0)(3,"div",3),Ge(4,aH,0,0,"ng-template",4),h(),M(5,"div"),h()()),i&2&&(p(5),G("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[tr],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2,changeDetection:1})}return t})(),hH=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new nc}),Ik=(()=>{class t{_live=d(Bl);_injector=d(q);_breakpointObserver=d(Eb);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(hH);_animationsDisabled=Ae();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=mH;snackBarContainerComponent=fH;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=b(b({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=q.create({parent:r||this._injector,providers:[{provide:nc,useValue:i}]}),s=new Ei(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=b(b(b({},new nc),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new dd(s,o);if(e instanceof gt){let c=new Un(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(c)}else{let c=this._createInjector(r,a),l=new Ei(e,void 0,c),u=s.attachComponentPortal(l);a.instance=u.instance}return this._breakpointObserver.observe(MS.HandsetPortrait).pipe(pe(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Si;i.direction=e.direction;let r=Ms(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,nr(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return q.create({parent:r||this._injector,providers:[{provide:dd,useValue:i},{provide:Sk,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Eo=class t{snackbar=d(Ik);error(n){this.snackbar.open(n,"Close",{duration:5e3,panelClass:["snack-error"]})}success(n){this.snackbar.open(n,"Close",{duration:5e3,panelClass:["snack-success"]})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};var pH=["*"],wh=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&H("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:pH,decls:1,vars:0,template:function(i,r){i&1&&(ke(),ie(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})}return t})();var gH=["input"],_H=["label"],vH=["*"],Ey={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},bH=new y("mat-checkbox-default-options",{providedIn:"root",factory:()=>Ey}),dn=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(dn||{}),Sy=class{source;checked},Iy=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(ge);_ngZone=d(B);_animationsDisabled=Ae();_options=d(bH,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new Sy;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new U;indeterminateChange=new U;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=dn.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(qe).load(Rn);let e=d(new ti("tabindex"),{optional:!0});this._options=this._options||Ey,this.color=this._options.color||Ey.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(Te).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(dn.Indeterminate):this._transitionCheckState(this.checked?dn.Checked:dn.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=V(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?dn.Checked:dn.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case dn.Init:if(i===dn.Checked)return this._animationClasses.uncheckedToChecked;if(i==dn.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case dn.Unchecked:return i===dn.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case dn.Checked:return i===dn.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case dn.Indeterminate:return i===dn.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&je(gH,5)(_H,5),i&2){let o;z(o=$())&&(r._inputElement=o.first),z(o=$())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(ht("id",r.id),G("tabindex",null)("aria-label",null)("aria-labelledby",null),jt(r.color?"mat-"+r.color:"mat-accent"),H("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",L],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",L],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",L],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:Et(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",L],checked:[2,"checked","checked",L],disabled:[2,"disabled","disabled",L],indeterminate:[2,"indeterminate","indeterminate",L]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Oe([{provide:ir,useExisting:Mt(()=>t),multi:!0},{provide:vo,useExisting:t,multi:!0}]),$e],ngContentSelectors:vH,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(ke(),f(0,"div",3),A("click",function(s){return r._preventBubblingFromLabel(s)}),f(1,"div",4,0)(3,"div",5),A("click",function(){return r._onTouchTargetClick()}),h(),f(4,"input",6,1),A("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),h(),M(6,"div",7),f(7,"div",8),$t(),f(8,"svg",9),M(9,"path",10),h(),$i(),M(10,"div",11),h(),M(11,"div",12),h(),f(12,"label",13,2),ie(14),h()()),i&2){let o=He(2);x("labelPosition",r.labelPosition),p(4),H("mdc-checkbox--selected",r.checked),x("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),G("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),p(7),x("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),p(),x("for",r.inputId)}},dependencies:[er,wh],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--mat-checkbox-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-checkbox-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-checkbox-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-checkbox-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-checkbox-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-checkbox-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--mat-checkbox-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-checkbox-touch-target-size, 48px);
  width: var(--mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})(),kk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Iy,Wt]})}return t})();var $n=class extends Error{constructor(n,e){let i=new.target.prototype;super(`${n}: Status code '${e}'`),this.statusCode=e,this.__proto__=i}},So=class extends Error{constructor(n="A timeout occurred."){let e=new.target.prototype;super(n),this.__proto__=e}},qt=class extends Error{constructor(n="An abort occurred."){let e=new.target.prototype;super(n),this.__proto__=e}},xh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="UnsupportedTransportError",this.__proto__=i}},Dh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="DisabledTransportError",this.__proto__=i}},Eh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="FailedToStartTransportError",this.__proto__=i}},ud=class extends Error{constructor(n){let e=new.target.prototype;super(n),this.errorType="FailedToNegotiateWithServerError",this.__proto__=e}},Sh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.innerErrors=e,this.__proto__=i}};var ic=class{constructor(n,e,i){this.statusCode=n,this.statusText=e,this.content=i}},sr=class{get(n,e){return this.send(Q(b({},e),{method:"GET",url:n}))}post(n,e){return this.send(Q(b({},e),{method:"POST",url:n}))}delete(n,e){return this.send(Q(b({},e),{method:"DELETE",url:n}))}getCookieString(n){return""}};var w=(function(t){return t[t.Trace=0]="Trace",t[t.Debug=1]="Debug",t[t.Information=2]="Information",t[t.Warning=3]="Warning",t[t.Error=4]="Error",t[t.Critical=5]="Critical",t[t.None=6]="None",t})(w||{});var ar=class{constructor(){}log(n,e){}};ar.instance=new ar;var Mk="10.0.11";var Ye=class{static isRequired(n,e){if(n==null)throw new Error(`The '${e}' argument is required.`)}static isNotEmpty(n,e){if(!n||n.match(/^\s*$/))throw new Error(`The '${e}' argument should not be empty.`)}static isIn(n,e,i){if(!(n in e))throw new Error(`Unknown ${i} value: ${n}.`)}},Xe=class t{static get isBrowser(){return!t.isNode&&typeof window=="object"&&typeof window.document=="object"}static get isWebWorker(){return!t.isNode&&typeof self=="object"&&"importScripts"in self}static get isReactNative(){return!t.isNode&&typeof window=="object"&&typeof window.document>"u"}static get isNode(){return typeof process<"u"&&process.release&&process.release.name==="node"}};function Io(t,n){let e="";return ki(t)?(e=`Binary data of length ${t.byteLength}`,n&&(e+=`. Content: '${CH(t)}'`)):typeof t=="string"&&(e=`String data of length ${t.length}`,n&&(e+=`. Content: '${t}'`)),e}function CH(t){let n=new Uint8Array(t),e="";return n.forEach(i=>{let r=i<16?"0":"";e+=`0x${r}${i.toString(16)} `}),e.substring(0,e.length-1)}function ki(t){return t&&typeof ArrayBuffer<"u"&&(t instanceof ArrayBuffer||t.constructor&&t.constructor.name==="ArrayBuffer")}async function kh(t,n,e,i,r,o){let s={},[a,c]=cr();s[a]=c,t.log(w.Trace,`(${n} transport) sending data. ${Io(r,o.logMessageContent)}.`);let l=ki(r)?"arraybuffer":"text",u=await e.post(i,{content:r,headers:b(b({},s),o.headers),responseType:l,timeout:o.timeout,withCredentials:o.withCredentials});t.log(w.Trace,`(${n} transport) request complete. Response status: ${u.statusCode}.`)}function Tk(t){return t===void 0?new Os(w.Information):t===null?ar.instance:t.log!==void 0?t:new Os(t)}var Ih=class{constructor(n,e){this._subject=n,this._observer=e}dispose(){let n=this._subject.observers.indexOf(this._observer);n>-1&&this._subject.observers.splice(n,1),this._subject.observers.length===0&&this._subject.cancelCallback&&this._subject.cancelCallback().catch(e=>{})}},Os=class{constructor(n){this._minLevel=n,this.out=console}log(n,e){if(n>=this._minLevel){let i=`[${new Date().toISOString()}] ${w[n]}: ${e}`;switch(n){case w.Critical:case w.Error:this.out.error(i);break;case w.Warning:this.out.warn(i);break;case w.Information:this.out.info(i);break;default:this.out.log(i);break}}}};function cr(){let t="X-SignalR-User-Agent";return Xe.isNode&&(t="User-Agent"),[t,wH(Mk,xH(),EH(),DH())]}function wH(t,n,e,i){let r="Microsoft SignalR/",o=t.split(".");return r+=`${o[0]}.${o[1]}`,r+=` (${t}; `,n&&n!==""?r+=`${n}; `:r+="Unknown OS; ",r+=`${e}`,i?r+=`; ${i}`:r+="; Unknown Runtime Version",r+=")",r}function xH(){if(Xe.isNode)switch(process.platform){case"win32":return"Windows NT";case"darwin":return"macOS";case"linux":return"Linux";default:return process.platform}else return""}function DH(){if(Xe.isNode)return process.versions.node}function EH(){return Xe.isNode?"NodeJS":"Browser"}function Mh(t){return t.stack?t.stack:t.message?t.message:`${t}`}function Ak(){if(typeof globalThis<"u")return globalThis;if(typeof self<"u")return self;if(typeof window<"u")return window;if(typeof global<"u")return global;throw new Error("could not find global")}var Th=class extends sr{constructor(n){if(super(),this._logger=n,typeof fetch>"u"||Xe.isNode){let e=typeof __webpack_require__=="function"?__non_webpack_require__:Ls;this._jar=new(e("tough-cookie")).CookieJar,typeof fetch>"u"?this._fetchType=e("node-fetch"):this._fetchType=fetch,this._fetchType=e("fetch-cookie")(this._fetchType,this._jar)}else this._fetchType=fetch.bind(Ak());if(typeof AbortController>"u"){let e=typeof __webpack_require__=="function"?__non_webpack_require__:Ls;this._abortControllerType=e("abort-controller")}else this._abortControllerType=AbortController}async send(n){if(n.abortSignal&&n.abortSignal.aborted)throw new qt;if(!n.method)throw new Error("No method defined.");if(!n.url)throw new Error("No url defined.");let e=new this._abortControllerType,i;n.abortSignal&&(n.abortSignal.onabort=()=>{e.abort(),i=new qt});let r=null;if(n.timeout){let c=n.timeout;r=setTimeout(()=>{e.abort(),this._logger.log(w.Warning,"Timeout from HTTP request."),i=new So},c)}n.content===""&&(n.content=void 0),n.content&&(n.headers=n.headers||{},ki(n.content)?n.headers["Content-Type"]="application/octet-stream":n.headers["Content-Type"]="text/plain;charset=UTF-8");let o;try{o=await this._fetchType(n.url,{body:n.content,cache:"no-cache",credentials:n.withCredentials===!0?"include":"same-origin",headers:b({"X-Requested-With":"XMLHttpRequest"},n.headers),method:n.method,mode:"cors",redirect:"follow",signal:e.signal})}catch(c){throw i||(this._logger.log(w.Warning,`Error from HTTP request. ${c}.`),c)}finally{r&&clearTimeout(r),n.abortSignal&&(n.abortSignal.onabort=null)}if(!o.ok){let c=await Rk(o,"text");throw new $n(c||o.statusText,o.status)}let a=await Rk(o,n.responseType);return new ic(o.status,o.statusText,a)}getCookieString(n){let e="";return Xe.isNode&&this._jar&&this._jar.getCookies(n,(i,r)=>e=r.join("; ")),e}};function Rk(t,n){let e;switch(n){case"arraybuffer":e=t.arrayBuffer();break;case"text":e=t.text();break;case"blob":case"document":case"json":throw new Error(`${n} is not supported.`);default:e=t.text();break}return e}var Ah=class extends sr{constructor(n){super(),this._logger=n}send(n){return n.abortSignal&&n.abortSignal.aborted?Promise.reject(new qt):n.method?n.url?new Promise((e,i)=>{let r=new XMLHttpRequest;r.open(n.method,n.url,!0),r.withCredentials=n.withCredentials===void 0?!0:n.withCredentials,r.setRequestHeader("X-Requested-With","XMLHttpRequest"),n.content===""&&(n.content=void 0),n.content&&(ki(n.content)?r.setRequestHeader("Content-Type","application/octet-stream"):r.setRequestHeader("Content-Type","text/plain;charset=UTF-8"));let o=n.headers;o&&Object.keys(o).forEach(s=>{r.setRequestHeader(s,o[s])}),n.responseType&&(r.responseType=n.responseType),n.abortSignal&&(n.abortSignal.onabort=()=>{r.abort(),i(new qt)}),n.timeout&&(r.timeout=n.timeout),r.onload=()=>{n.abortSignal&&(n.abortSignal.onabort=null),r.status>=200&&r.status<300?e(new ic(r.status,r.statusText,r.response||r.responseText)):i(new $n(r.response||r.responseText||r.statusText,r.status))},r.onerror=()=>{this._logger.log(w.Warning,`Error from HTTP request. ${r.status}: ${r.statusText}.`),i(new $n(r.statusText,r.status))},r.ontimeout=()=>{this._logger.log(w.Warning,"Timeout from HTTP request."),i(new So)},r.send(n.content)}):Promise.reject(new Error("No url defined.")):Promise.reject(new Error("No method defined."))}};var Rh=class extends sr{constructor(n){if(super(),typeof fetch<"u"||Xe.isNode)this._httpClient=new Th(n);else if(typeof XMLHttpRequest<"u")this._httpClient=new Ah(n);else throw new Error("No usable HttpClient found.")}send(n){return n.abortSignal&&n.abortSignal.aborted?Promise.reject(new qt):n.method?n.url?this._httpClient.send(n):Promise.reject(new Error("No url defined.")):Promise.reject(new Error("No method defined."))}getCookieString(n){return this._httpClient.getCookieString(n)}};var Gn=class t{static write(n){return`${n}${t.RecordSeparator}`}static parse(n){if(n[n.length-1]!==t.RecordSeparator)throw new Error("Message is incomplete.");let e=n.split(t.RecordSeparator);return e.pop(),e}};Gn.RecordSeparatorCode=30;Gn.RecordSeparator=String.fromCharCode(Gn.RecordSeparatorCode);var Nh=class{writeHandshakeRequest(n){return Gn.write(JSON.stringify(n))}parseHandshakeResponse(n){let e,i;if(ki(n)){let a=new Uint8Array(n),c=a.indexOf(Gn.RecordSeparatorCode);if(c===-1)throw new Error("Message is incomplete.");let l=c+1;e=String.fromCharCode.apply(null,Array.prototype.slice.call(a.slice(0,l))),i=a.byteLength>l?a.slice(l).buffer:null}else{let a=n,c=a.indexOf(Gn.RecordSeparator);if(c===-1)throw new Error("Message is incomplete.");let l=c+1;e=a.substring(0,l),i=a.length>l?a.substring(l):null}let r=Gn.parse(e),o=JSON.parse(r[0]);if(o.type)throw new Error("Expected a handshake response from the server.");return[i,o]}};var ue=(function(t){return t[t.Invocation=1]="Invocation",t[t.StreamItem=2]="StreamItem",t[t.Completion=3]="Completion",t[t.StreamInvocation=4]="StreamInvocation",t[t.CancelInvocation=5]="CancelInvocation",t[t.Ping=6]="Ping",t[t.Close=7]="Close",t[t.Ack=8]="Ack",t[t.Sequence=9]="Sequence",t})(ue||{});var Oh=class{constructor(){this.observers=[]}next(n){for(let e of this.observers)e.next(n)}error(n){for(let e of this.observers)e.error&&e.error(n)}complete(){for(let n of this.observers)n.complete&&n.complete()}subscribe(n){return this.observers.push(n),new Ih(this,n)}};var Ph=class{constructor(n,e,i){this._bufferSize=1e5,this._messages=[],this._totalMessageCount=0,this._waitForSequenceMessage=!1,this._nextReceivingSequenceId=1,this._latestReceivedSequenceId=0,this._bufferedByteCount=0,this._reconnectInProgress=!1,this._protocol=n,this._connection=e,this._bufferSize=i}async _send(n){let e=this._protocol.writeMessage(n),i=Promise.resolve();if(this._isInvocationMessage(n)){this._totalMessageCount++;let r=()=>{},o=()=>{};ki(e)?this._bufferedByteCount+=e.byteLength:this._bufferedByteCount+=e.length,this._bufferedByteCount>=this._bufferSize&&(i=new Promise((s,a)=>{r=s,o=a})),this._messages.push(new ky(e,this._totalMessageCount,r,o))}try{this._reconnectInProgress||await this._connection.send(e)}catch{this._disconnected()}await i}_ack(n){let e=-1;for(let i=0;i<this._messages.length;i++){let r=this._messages[i];if(r._id<=n.sequenceId)e=i,ki(r._message)?this._bufferedByteCount-=r._message.byteLength:this._bufferedByteCount-=r._message.length,r._resolver();else if(this._bufferedByteCount<this._bufferSize)r._resolver();else break}e!==-1&&(this._messages=this._messages.slice(e+1))}_shouldProcessMessage(n){if(this._waitForSequenceMessage)return n.type!==ue.Sequence?!1:(this._waitForSequenceMessage=!1,!0);if(!this._isInvocationMessage(n))return!0;let e=this._nextReceivingSequenceId;return this._nextReceivingSequenceId++,e<=this._latestReceivedSequenceId?(e===this._latestReceivedSequenceId&&this._ackTimer(),!1):(this._latestReceivedSequenceId=e,this._ackTimer(),!0)}_resetSequence(n){if(n.sequenceId>this._nextReceivingSequenceId){this._connection.stop(new Error("Sequence ID greater than amount of messages we've received."));return}this._nextReceivingSequenceId=n.sequenceId}_disconnected(){this._reconnectInProgress=!0,this._waitForSequenceMessage=!0}async _resend(){let n=this._messages.length!==0?this._messages[0]._id:this._totalMessageCount+1;await this._connection.send(this._protocol.writeMessage({type:ue.Sequence,sequenceId:n}));let e=this._messages;for(let i of e)await this._connection.send(i._message);this._reconnectInProgress=!1}_dispose(n){n??(n=new Error("Unable to reconnect to server."));for(let e of this._messages)e._rejector(n)}_isInvocationMessage(n){switch(n.type){case ue.Invocation:case ue.StreamItem:case ue.Completion:case ue.StreamInvocation:case ue.CancelInvocation:return!0;case ue.Close:case ue.Sequence:case ue.Ping:case ue.Ack:return!1}}_ackTimer(){this._ackTimerHandle===void 0&&(this._ackTimerHandle=setTimeout(async()=>{try{this._reconnectInProgress||await this._connection.send(this._protocol.writeMessage({type:ue.Ack,sequenceId:this._latestReceivedSequenceId}))}catch{}clearTimeout(this._ackTimerHandle),this._ackTimerHandle=void 0},1e3))}},ky=class{constructor(n,e,i,r){this._message=n,this._id=e,this._resolver=i,this._rejector=r}};var SH=30*1e3,IH=15*1e3,kH=1e5,Je=(function(t){return t.Disconnected="Disconnected",t.Connecting="Connecting",t.Connected="Connected",t.Disconnecting="Disconnecting",t.Reconnecting="Reconnecting",t})(Je||{}),md=class t{static create(n,e,i,r,o,s,a){return new t(n,e,i,r,o,s,a)}constructor(n,e,i,r,o,s,a){this._nextKeepAlive=0,this._freezeEventListener=()=>{this._logger.log(w.Warning,"The page is being frozen, this will likely lead to the connection being closed and messages being lost. For more information see the docs at https://learn.microsoft.com/aspnet/core/signalr/javascript-client#bsleep")},Ye.isRequired(n,"connection"),Ye.isRequired(e,"logger"),Ye.isRequired(i,"protocol"),this.serverTimeoutInMilliseconds=o??SH,this.keepAliveIntervalInMilliseconds=s??IH,this._statefulReconnectBufferSize=a??kH,this._logger=e,this._protocol=i,this.connection=n,this._reconnectPolicy=r,this._handshakeProtocol=new Nh,this.connection.onreceive=c=>this._processIncomingData(c),this.connection.onclose=c=>this._connectionClosed(c),this._callbacks={},this._methods={},this._closedCallbacks=[],this._reconnectingCallbacks=[],this._reconnectedCallbacks=[],this._invocationId=0,this._receivedHandshakeResponse=!1,this._connectionState=Je.Disconnected,this._connectionStarted=!1,this._cachedPingMessage=this._protocol.writeMessage({type:ue.Ping})}get state(){return this._connectionState}get connectionId(){return this.connection&&this.connection.connectionId||null}get baseUrl(){return this.connection.baseUrl||""}set baseUrl(n){if(this._connectionState!==Je.Disconnected&&this._connectionState!==Je.Reconnecting)throw new Error("The HubConnection must be in the Disconnected or Reconnecting state to change the url.");if(!n)throw new Error("The HubConnection url must be a valid url.");this.connection.baseUrl=n}start(){return this._startPromise=this._startWithStateTransitions(),this._startPromise}async _startWithStateTransitions(){if(this._connectionState!==Je.Disconnected)return Promise.reject(new Error("Cannot start a HubConnection that is not in the 'Disconnected' state."));this._connectionState=Je.Connecting,this._logger.log(w.Debug,"Starting HubConnection.");try{await this._startInternal(),Xe.isBrowser&&window.document.addEventListener("freeze",this._freezeEventListener),this._connectionState=Je.Connected,this._connectionStarted=!0,this._logger.log(w.Debug,"HubConnection connected successfully.")}catch(n){return this._connectionState=Je.Disconnected,this._logger.log(w.Debug,`HubConnection failed to start successfully because of error '${n}'.`),Promise.reject(n)}}async _startInternal(){this._stopDuringStartError=void 0,this._receivedHandshakeResponse=!1;let n=new Promise((e,i)=>{this._handshakeResolver=e,this._handshakeRejecter=i});await this.connection.start(this._protocol.transferFormat);try{let e=this._protocol.version;this.connection.features.reconnect||(e=1);let i={protocol:this._protocol.name,version:e};if(this._logger.log(w.Debug,"Sending handshake request."),await this._sendMessage(this._handshakeProtocol.writeHandshakeRequest(i)),this._logger.log(w.Information,`Using HubProtocol '${this._protocol.name}'.`),this._cleanupTimeout(),this._resetTimeoutPeriod(),this._resetKeepAliveInterval(),await n,this._stopDuringStartError)throw this._stopDuringStartError;this.connection.features.reconnect&&(this._messageBuffer=new Ph(this._protocol,this.connection,this._statefulReconnectBufferSize),this.connection.features.disconnected=this._messageBuffer._disconnected.bind(this._messageBuffer),this.connection.features.resend=()=>{if(this._messageBuffer)return this._messageBuffer._resend()}),this.connection.features.inherentKeepAlive||await this._sendMessage(this._cachedPingMessage)}catch(e){throw this._logger.log(w.Debug,`Hub handshake failed with error '${e}' during start(). Stopping HubConnection.`),this._cleanupTimeout(),this._cleanupPingTimer(),await this.connection.stop(e),e}}async stop(){let n=this._startPromise;this.connection.features.reconnect=!1,this._stopPromise=this._stopInternal(),await this._stopPromise;try{await n}catch{}}_stopInternal(n){if(this._connectionState===Je.Disconnected)return this._logger.log(w.Debug,`Call to HubConnection.stop(${n}) ignored because it is already in the disconnected state.`),Promise.resolve();if(this._connectionState===Je.Disconnecting)return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnecting state.`),this._stopPromise;let e=this._connectionState;return this._connectionState=Je.Disconnecting,this._logger.log(w.Debug,"Stopping HubConnection."),this._reconnectDelayHandle?(this._logger.log(w.Debug,"Connection stopped during reconnect delay. Done reconnecting."),clearTimeout(this._reconnectDelayHandle),this._reconnectDelayHandle=void 0,this._completeClose(),Promise.resolve()):(e===Je.Connected&&this._sendCloseMessage(),this._cleanupTimeout(),this._cleanupPingTimer(),this._stopDuringStartError=n||new qt("The connection was stopped before the hub handshake could complete."),this.connection.stop(n))}async _sendCloseMessage(){try{await this._sendWithProtocol(this._createCloseMessage())}catch{}}stream(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._createStreamInvocation(n,e,r),s,a=new Oh;return a.cancelCallback=()=>{let c=this._createCancelInvocation(o.invocationId);return delete this._callbacks[o.invocationId],s.then(()=>this._sendWithProtocol(c))},this._callbacks[o.invocationId]=(c,l)=>{if(l){a.error(l);return}else c&&(c.type===ue.Completion?c.error?a.error(new Error(c.error)):a.complete():a.next(c.item))},s=this._sendWithProtocol(o).catch(c=>{a.error(c),delete this._callbacks[o.invocationId]}),this._launchStreams(i,s),a}_sendMessage(n){return this._resetKeepAliveInterval(),this.connection.send(n)}_sendWithProtocol(n){return this._messageBuffer?this._messageBuffer._send(n):this._sendMessage(this._protocol.writeMessage(n))}send(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._sendWithProtocol(this._createInvocation(n,e,!0,r));return this._launchStreams(i,o),o}invoke(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._createInvocation(n,e,!1,r);return new Promise((a,c)=>{this._callbacks[o.invocationId]=(u,m)=>{if(m){c(m);return}else u&&(u.type===ue.Completion?u.error?c(new Error(u.error)):a(u.result):c(new Error(`Unexpected message type: ${u.type}`)))};let l=this._sendWithProtocol(o).catch(u=>{c(u),delete this._callbacks[o.invocationId]});this._launchStreams(i,l)})}on(n,e){!n||!e||(n=n.toLowerCase(),this._methods[n]||(this._methods[n]=[]),this._methods[n].indexOf(e)===-1&&this._methods[n].push(e))}off(n,e){if(!n)return;n=n.toLowerCase();let i=this._methods[n];if(i)if(e){let r=i.indexOf(e);r!==-1&&(i.splice(r,1),i.length===0&&delete this._methods[n])}else delete this._methods[n]}onclose(n){n&&this._closedCallbacks.push(n)}onreconnecting(n){n&&this._reconnectingCallbacks.push(n)}onreconnected(n){n&&this._reconnectedCallbacks.push(n)}_processIncomingData(n){if(this._cleanupTimeout(),this._receivedHandshakeResponse||(n=this._processHandshakeResponse(n),this._receivedHandshakeResponse=!0),n){let e=this._protocol.parseMessages(n,this._logger);for(let i of e)if(!(this._messageBuffer&&!this._messageBuffer._shouldProcessMessage(i)))switch(i.type){case ue.Invocation:this._invokeClientMethod(i).catch(r=>{this._logger.log(w.Error,`Invoke client method threw error: ${Mh(r)}`)});break;case ue.StreamItem:case ue.Completion:{let r=this._callbacks[i.invocationId];if(r){i.type===ue.Completion&&delete this._callbacks[i.invocationId];try{r(i)}catch(o){this._logger.log(w.Error,`Stream callback threw error: ${Mh(o)}`)}}break}case ue.Ping:break;case ue.Close:{this._logger.log(w.Information,"Close message received from server.");let r=i.error?new Error("Server returned an error on close: "+i.error):void 0;i.allowReconnect===!0?this.connection.stop(r):this._stopPromise=this._stopInternal(r);break}case ue.Ack:this._messageBuffer&&this._messageBuffer._ack(i);break;case ue.Sequence:this._messageBuffer&&this._messageBuffer._resetSequence(i);break;default:this._logger.log(w.Warning,`Invalid message type: ${i.type}.`);break}}this._resetTimeoutPeriod()}_processHandshakeResponse(n){let e,i;try{[i,e]=this._handshakeProtocol.parseHandshakeResponse(n)}catch(r){let o="Error parsing handshake response: "+r;this._logger.log(w.Error,o);let s=new Error(o);throw this._handshakeRejecter(s),s}if(e.error){let r="Server returned handshake error: "+e.error;this._logger.log(w.Error,r);let o=new Error(r);throw this._handshakeRejecter(o),o}else this._logger.log(w.Debug,"Server handshake complete.");return this._handshakeResolver(),i}_resetKeepAliveInterval(){this.connection.features.inherentKeepAlive||(this._nextKeepAlive=new Date().getTime()+this.keepAliveIntervalInMilliseconds,this._cleanupPingTimer())}_resetTimeoutPeriod(){if(!this.connection.features||!this.connection.features.inherentKeepAlive){this._timeoutHandle=setTimeout(()=>this.serverTimeout(),this.serverTimeoutInMilliseconds);let n=this._nextKeepAlive-new Date().getTime();if(n<0){this._connectionState===Je.Connected&&this._trySendPingMessage();return}this._pingServerHandle===void 0&&(n<0&&(n=0),this._pingServerHandle=setTimeout(async()=>{this._connectionState===Je.Connected&&await this._trySendPingMessage()},n))}}serverTimeout(){this.connection.stop(new Error("Server timeout elapsed without receiving a message from the server."))}async _invokeClientMethod(n){let e=n.target.toLowerCase(),i=this._methods[e];if(!i){this._logger.log(w.Warning,`No client method with the name '${e}' found.`),n.invocationId&&(this._logger.log(w.Warning,`No result given for '${e}' method and invocation ID '${n.invocationId}'.`),await this._sendWithProtocol(this._createCompletionMessage(n.invocationId,"Client didn't provide a result.",null)));return}let r=i.slice(),o=!!n.invocationId,s,a,c;for(let l of r)try{let u=s;s=await l.apply(this,n.arguments),o&&s&&u&&(this._logger.log(w.Error,`Multiple results provided for '${e}'. Sending error to server.`),c=this._createCompletionMessage(n.invocationId,"Client provided multiple results.",null)),a=void 0}catch(u){a=u,this._logger.log(w.Error,`A callback for the method '${e}' threw error '${u}'.`)}c?await this._sendWithProtocol(c):o?(a?c=this._createCompletionMessage(n.invocationId,`${a}`,null):s!==void 0?c=this._createCompletionMessage(n.invocationId,null,s):(this._logger.log(w.Warning,`No result given for '${e}' method and invocation ID '${n.invocationId}'.`),c=this._createCompletionMessage(n.invocationId,"Client didn't provide a result.",null)),await this._sendWithProtocol(c)):s&&this._logger.log(w.Error,`Result given for '${e}' method but server is not expecting a result.`)}_connectionClosed(n){this._logger.log(w.Debug,`HubConnection.connectionClosed(${n}) called while in state ${this._connectionState}.`),this._stopDuringStartError=this._stopDuringStartError||n||new qt("The underlying connection was closed before the hub handshake could complete."),this._handshakeResolver&&this._handshakeResolver(),this._cancelCallbacksWithError(n||new Error("Invocation canceled due to the underlying connection being closed.")),this._cleanupTimeout(),this._cleanupPingTimer(),this._connectionState===Je.Disconnecting?this._completeClose(n):this._connectionState===Je.Connected&&this._reconnectPolicy?this._reconnect(n):this._connectionState===Je.Connected&&this._completeClose(n)}_completeClose(n){if(this._connectionStarted){this._connectionState=Je.Disconnected,this._connectionStarted=!1,this._messageBuffer&&(this._messageBuffer._dispose(n??new Error("Connection closed.")),this._messageBuffer=void 0),Xe.isBrowser&&window.document.removeEventListener("freeze",this._freezeEventListener);try{this._closedCallbacks.forEach(e=>e.apply(this,[n]))}catch(e){this._logger.log(w.Error,`An onclose callback called with error '${n}' threw error '${e}'.`)}}}async _reconnect(n){let e=Date.now(),i=0,r=n!==void 0?n:new Error("Attempting to reconnect due to a unknown error."),o=this._getNextRetryDelay(i,0,r);if(o===null){this._logger.log(w.Debug,"Connection not reconnecting because the IRetryPolicy returned null on the first reconnect attempt."),this._completeClose(n);return}if(this._connectionState=Je.Reconnecting,n?this._logger.log(w.Information,`Connection reconnecting because of error '${n}'.`):this._logger.log(w.Information,"Connection reconnecting."),this._reconnectingCallbacks.length!==0){try{this._reconnectingCallbacks.forEach(s=>s.apply(this,[n]))}catch(s){this._logger.log(w.Error,`An onreconnecting callback called with error '${n}' threw error '${s}'.`)}if(this._connectionState!==Je.Reconnecting){this._logger.log(w.Debug,"Connection left the reconnecting state in onreconnecting callback. Done reconnecting.");return}}for(;o!==null;){if(this._logger.log(w.Information,`Reconnect attempt number ${i+1} will start in ${o} ms.`),await new Promise(s=>{this._reconnectDelayHandle=setTimeout(s,o)}),this._reconnectDelayHandle=void 0,this._connectionState!==Je.Reconnecting){this._logger.log(w.Debug,"Connection left the reconnecting state during reconnect delay. Done reconnecting.");return}try{if(await this._startInternal(),this._connectionState=Je.Connected,this._logger.log(w.Information,"HubConnection reconnected successfully."),this._reconnectedCallbacks.length!==0)try{this._reconnectedCallbacks.forEach(s=>s.apply(this,[this.connection.connectionId]))}catch(s){this._logger.log(w.Error,`An onreconnected callback called with connectionId '${this.connection.connectionId}; threw error '${s}'.`)}return}catch(s){if(this._logger.log(w.Information,`Reconnect attempt failed because of error '${s}'.`),this._connectionState!==Je.Reconnecting){this._logger.log(w.Debug,`Connection moved to the '${this._connectionState}' from the reconnecting state during reconnect attempt. Done reconnecting.`),this._connectionState===Je.Disconnecting&&this._completeClose();return}i++,r=s instanceof Error?s:new Error(s.toString()),o=this._getNextRetryDelay(i,Date.now()-e,r)}}this._logger.log(w.Information,`Reconnect retries have been exhausted after ${Date.now()-e} ms and ${i} failed attempts. Connection disconnecting.`),this._completeClose()}_getNextRetryDelay(n,e,i){try{return this._reconnectPolicy.nextRetryDelayInMilliseconds({elapsedMilliseconds:e,previousRetryCount:n,retryReason:i})}catch(r){return this._logger.log(w.Error,`IRetryPolicy.nextRetryDelayInMilliseconds(${n}, ${e}) threw error '${r}'.`),null}}_cancelCallbacksWithError(n){let e=this._callbacks;this._callbacks={},Object.keys(e).forEach(i=>{let r=e[i];try{r(null,n)}catch(o){this._logger.log(w.Error,`Stream 'error' callback called with '${n}' threw error: ${Mh(o)}`)}})}_cleanupPingTimer(){this._pingServerHandle&&(clearTimeout(this._pingServerHandle),this._pingServerHandle=void 0)}_cleanupTimeout(){this._timeoutHandle&&clearTimeout(this._timeoutHandle)}_createInvocation(n,e,i,r){if(i)return r.length!==0?{target:n,arguments:e,streamIds:r,type:ue.Invocation}:{target:n,arguments:e,type:ue.Invocation};{let o=this._invocationId;return this._invocationId++,r.length!==0?{target:n,arguments:e,invocationId:o.toString(),streamIds:r,type:ue.Invocation}:{target:n,arguments:e,invocationId:o.toString(),type:ue.Invocation}}}_launchStreams(n,e){if(n.length!==0){e||(e=Promise.resolve());for(let i in n)n[i].subscribe({complete:()=>{e=e.then(()=>this._sendWithProtocol(this._createCompletionMessage(i)))},error:r=>{let o;r instanceof Error?o=r.message:r&&r.toString?o=r.toString():o="Unknown error",e=e.then(()=>this._sendWithProtocol(this._createCompletionMessage(i,o)))},next:r=>{e=e.then(()=>this._sendWithProtocol(this._createStreamItemMessage(i,r)))}})}}_replaceStreamingParams(n){let e=[],i=[];for(let r=0;r<n.length;r++){let o=n[r];if(this._isObservable(o)){let s=this._invocationId;this._invocationId++,e[s]=o,i.push(s.toString()),n.splice(r,1)}}return[e,i]}_isObservable(n){return n&&n.subscribe&&typeof n.subscribe=="function"}_createStreamInvocation(n,e,i){let r=this._invocationId;return this._invocationId++,i.length!==0?{target:n,arguments:e,invocationId:r.toString(),streamIds:i,type:ue.StreamInvocation}:{target:n,arguments:e,invocationId:r.toString(),type:ue.StreamInvocation}}_createCancelInvocation(n){return{invocationId:n,type:ue.CancelInvocation}}_createStreamItemMessage(n,e){return{invocationId:n,item:e,type:ue.StreamItem}}_createCompletionMessage(n,e,i){return e?{error:e,invocationId:n,type:ue.Completion}:{invocationId:n,result:i,type:ue.Completion}}_createCloseMessage(){return{type:ue.Close}}async _trySendPingMessage(){try{await this._sendMessage(this._cachedPingMessage)}catch{this._cleanupPingTimer()}}};var MH=[0,2e3,1e4,3e4,null],fd=class{constructor(n){this._retryDelays=n!==void 0?[...n,null]:MH}nextRetryDelayInMilliseconds(n){return this._retryDelays[n.previousRetryCount]}};var Ps=(()=>{class t{}return t.Authorization="Authorization",t.Cookie="Cookie",t})();var Fh=class extends sr{constructor(n,e){super(),this._innerClient=n,this._accessTokenFactory=e}async send(n){let e=!0;this._accessTokenFactory&&(!this._accessToken||n.url&&n.url.indexOf("/negotiate?")>0)&&(e=!1,this._accessToken=await this._accessTokenFactory()),this._setAuthorizationHeader(n);let i=await this._innerClient.send(n);return e&&i.statusCode===401&&this._accessTokenFactory?(this._accessToken=await this._accessTokenFactory(),this._setAuthorizationHeader(n),await this._innerClient.send(n)):i}_setAuthorizationHeader(n){n.headers||(n.headers={}),this._accessToken?n.headers[Ps.Authorization]=`Bearer ${this._accessToken}`:this._accessTokenFactory&&n.headers[Ps.Authorization]&&delete n.headers[Ps.Authorization]}getCookieString(n){return this._innerClient.getCookieString(n)}};var Ut=(function(t){return t[t.None=0]="None",t[t.WebSockets=1]="WebSockets",t[t.ServerSentEvents=2]="ServerSentEvents",t[t.LongPolling=4]="LongPolling",t})(Ut||{}),Ot=(function(t){return t[t.Text=1]="Text",t[t.Binary=2]="Binary",t})(Ot||{});var Lh=class{constructor(){this._isAborted=!1,this.onabort=null}abort(){this._isAborted||(this._isAborted=!0,this.onabort&&this.onabort())}get signal(){return this}get aborted(){return this._isAborted}};var hd=class{get pollAborted(){return this._pollAbort.aborted}constructor(n,e,i){this._httpClient=n,this._logger=e,this._pollAbort=new Lh,this._options=i,this._running=!1,this.onreceive=null,this.onclose=null}async connect(n,e){if(Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Ot,"transferFormat"),this._url=n,this._logger.log(w.Trace,"(LongPolling transport) Connecting."),e===Ot.Binary&&typeof XMLHttpRequest<"u"&&typeof new XMLHttpRequest().responseType!="string")throw new Error("Binary protocols over XmlHttpRequest not implementing advanced features are not supported.");let[i,r]=cr(),o=b({[i]:r},this._options.headers),s={abortSignal:this._pollAbort.signal,headers:o,timeout:1e5,withCredentials:this._options.withCredentials};e===Ot.Binary&&(s.responseType="arraybuffer");let a=`${n}&_=${Date.now()}`;this._logger.log(w.Trace,`(LongPolling transport) polling: ${a}.`);let c=await this._httpClient.get(a,s);c.statusCode!==200?(this._logger.log(w.Error,`(LongPolling transport) Unexpected response code: ${c.statusCode}.`),this._closeError=new $n(c.statusText||"",c.statusCode),this._running=!1):this._running=!0,this._receiving=this._poll(this._url,s)}async _poll(n,e){try{for(;this._running;)try{let i=`${n}&_=${Date.now()}`;this._logger.log(w.Trace,`(LongPolling transport) polling: ${i}.`);let r=await this._httpClient.get(i,e);r.statusCode===204?(this._logger.log(w.Information,"(LongPolling transport) Poll terminated by server."),this._running=!1):r.statusCode!==200?(this._logger.log(w.Error,`(LongPolling transport) Unexpected response code: ${r.statusCode}.`),this._closeError=new $n(r.statusText||"",r.statusCode),this._running=!1):r.content?(this._logger.log(w.Trace,`(LongPolling transport) data received. ${Io(r.content,this._options.logMessageContent)}.`),this.onreceive&&this.onreceive(r.content)):this._logger.log(w.Trace,"(LongPolling transport) Poll timed out, reissuing.")}catch(i){this._running?i instanceof So?this._logger.log(w.Trace,"(LongPolling transport) Poll timed out, reissuing."):(this._closeError=i,this._running=!1):this._logger.log(w.Trace,`(LongPolling transport) Poll errored after shutdown: ${i.message}`)}}finally{this._logger.log(w.Trace,"(LongPolling transport) Polling complete."),this.pollAborted||this._raiseOnClose()}}async send(n){return this._running?kh(this._logger,"LongPolling",this._httpClient,this._url,n,this._options):Promise.reject(new Error("Cannot send until the transport is connected"))}async stop(){this._logger.log(w.Trace,"(LongPolling transport) Stopping polling."),this._running=!1,this._pollAbort.abort();try{await this._receiving,this._logger.log(w.Trace,`(LongPolling transport) sending DELETE request to ${this._url}.`);let n={},[e,i]=cr();n[e]=i;let r={headers:b(b({},n),this._options.headers),timeout:this._options.timeout,withCredentials:this._options.withCredentials},o;try{await this._httpClient.delete(this._url,r)}catch(s){o=s}o?o instanceof $n&&(o.statusCode===404?this._logger.log(w.Trace,"(LongPolling transport) A 404 response was returned from sending a DELETE request."):this._logger.log(w.Trace,`(LongPolling transport) Error sending a DELETE request: ${o}`)):this._logger.log(w.Trace,"(LongPolling transport) DELETE request accepted.")}finally{this._logger.log(w.Trace,"(LongPolling transport) Stop finished."),this._raiseOnClose()}}_raiseOnClose(){if(this.onclose){let n="(LongPolling transport) Firing onclose event.";this._closeError&&(n+=" Error: "+this._closeError),this._logger.log(w.Trace,n),this.onclose(this._closeError)}}};var Bh=class{constructor(n,e,i,r){this._httpClient=n,this._accessToken=e,this._logger=i,this._options=r,this.onreceive=null,this.onclose=null}async connect(n,e){return Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Ot,"transferFormat"),this._logger.log(w.Trace,"(SSE transport) Connecting."),this._url=n,this._accessToken&&(n+=(n.indexOf("?")<0?"?":"&")+`access_token=${encodeURIComponent(this._accessToken)}`),new Promise((i,r)=>{let o=!1;if(e!==Ot.Text){r(new Error("The Server-Sent Events transport only supports the 'Text' transfer format"));return}let s;if(Xe.isBrowser||Xe.isWebWorker)s=new this._options.EventSource(n,{withCredentials:this._options.withCredentials});else{let a=this._httpClient.getCookieString(n),c={};c.Cookie=a;let[l,u]=cr();c[l]=u,s=new this._options.EventSource(n,{withCredentials:this._options.withCredentials,headers:b(b({},c),this._options.headers)})}try{s.onmessage=a=>{if(this.onreceive)try{this._logger.log(w.Trace,`(SSE transport) data received. ${Io(a.data,this._options.logMessageContent)}.`),this.onreceive(a.data)}catch(c){this._close(c);return}},s.onerror=a=>{o?this._close():r(new Error("EventSource failed to connect. The connection could not be found on the server, either the connection ID is not present on the server, or a proxy is refusing/buffering the connection. If you have multiple servers check that sticky sessions are enabled."))},s.onopen=()=>{this._logger.log(w.Information,`SSE connected to ${this._url}`),this._eventSource=s,o=!0,i()}}catch(a){r(a);return}})}async send(n){return this._eventSource?kh(this._logger,"SSE",this._httpClient,this._url,n,this._options):Promise.reject(new Error("Cannot send until the transport is connected"))}stop(){return this._close(),Promise.resolve()}_close(n){this._eventSource&&(this._eventSource.close(),this._eventSource=void 0,this.onclose&&this.onclose(n))}};var Vh=class{constructor(n,e,i,r,o,s){this._logger=i,this._accessTokenFactory=e,this._logMessageContent=r,this._webSocketConstructor=o,this._httpClient=n,this.onreceive=null,this.onclose=null,this._headers=s}async connect(n,e){Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Ot,"transferFormat"),this._logger.log(w.Trace,"(WebSockets transport) Connecting.");let i;return this._accessTokenFactory&&(i=await this._accessTokenFactory()),new Promise((r,o)=>{n=n.replace(/^http/,"ws");let s,a=this._httpClient.getCookieString(n),c=!1;if(Xe.isNode||Xe.isReactNative){let l={},[u,m]=cr();l[u]=m,i&&(l[Ps.Authorization]=`Bearer ${i}`),a&&(l[Ps.Cookie]=a),s=new this._webSocketConstructor(n,void 0,{headers:b(b({},l),this._headers)})}else i&&(n+=(n.indexOf("?")<0?"?":"&")+`access_token=${encodeURIComponent(i)}`);s||(s=new this._webSocketConstructor(n)),e===Ot.Binary&&(s.binaryType="arraybuffer"),s.onopen=l=>{this._logger.log(w.Information,`WebSocket connected to ${n}.`),this._webSocket=s,c=!0,r()},s.onerror=l=>{let u=null;typeof ErrorEvent<"u"&&l instanceof ErrorEvent?u=l.error:u="There was an error with the transport",this._logger.log(w.Information,`(WebSockets transport) ${u}.`)},s.onmessage=l=>{if(this._logger.log(w.Trace,`(WebSockets transport) data received. ${Io(l.data,this._logMessageContent)}.`),this.onreceive)try{this.onreceive(l.data)}catch(u){this._close(u);return}},s.onclose=l=>{if(c)this._close(l);else{let u=null;typeof ErrorEvent<"u"&&l instanceof ErrorEvent?u=l.error:u="WebSocket failed to connect. The connection could not be found on the server, either the endpoint may not be a SignalR endpoint, the connection ID is not present on the server, or there is a proxy blocking WebSockets. If you have multiple servers check that sticky sessions are enabled.",o(new Error(u))}}})}send(n){return this._webSocket&&this._webSocket.readyState===this._webSocketConstructor.OPEN?(this._logger.log(w.Trace,`(WebSockets transport) sending data. ${Io(n,this._logMessageContent)}.`),this._webSocket.send(n),Promise.resolve()):Promise.reject("WebSocket is not in the OPEN state")}stop(){return this._webSocket&&this._close(void 0),Promise.resolve()}_close(n){this._webSocket&&(this._webSocket.onclose=()=>{},this._webSocket.onmessage=()=>{},this._webSocket.onerror=()=>{},this._webSocket.close(),this._webSocket=void 0),this._logger.log(w.Trace,"(WebSockets transport) socket closed."),this.onclose&&(this._isCloseEvent(n)&&(n.wasClean===!1||n.code!==1e3)?this.onclose(new Error(`WebSocket closed with status code: ${n.code} (${n.reason||"no reason given"}).`)):n instanceof Error?this.onclose(n):this.onclose())}_isCloseEvent(n){return n&&typeof n.wasClean=="boolean"&&typeof n.code=="number"}};var Nk=100,jh=class{constructor(n,e={}){if(this._stopPromiseResolver=()=>{},this.features={},this._negotiateVersion=1,Ye.isRequired(n,"url"),this._logger=Tk(e.logger),this.baseUrl=this._resolveUrl(n),e=e||{},e.logMessageContent=e.logMessageContent===void 0?!1:e.logMessageContent,typeof e.withCredentials=="boolean"||e.withCredentials===void 0)e.withCredentials=e.withCredentials===void 0?!0:e.withCredentials;else throw new Error("withCredentials option was not a 'boolean' or 'undefined' value");e.timeout=e.timeout===void 0?100*1e3:e.timeout;let i=null,r=null;if(Xe.isNode&&typeof Ls<"u"){let o=typeof __webpack_require__=="function"?__non_webpack_require__:Ls;i=o("ws"),r=o("eventsource")}!Xe.isNode&&typeof WebSocket<"u"&&!e.WebSocket?e.WebSocket=WebSocket:Xe.isNode&&!e.WebSocket&&i&&(e.WebSocket=i),!Xe.isNode&&typeof EventSource<"u"&&!e.EventSource?e.EventSource=EventSource:Xe.isNode&&!e.EventSource&&typeof r<"u"&&(e.EventSource=r),this._httpClient=new Fh(e.httpClient||new Rh(this._logger),e.accessTokenFactory),this._connectionState="Disconnected",this._connectionStarted=!1,this._options=e,this.onreceive=null,this.onclose=null}async start(n){if(n=n||Ot.Binary,Ye.isIn(n,Ot,"transferFormat"),this._logger.log(w.Debug,`Starting connection with transfer format '${Ot[n]}'.`),this._connectionState!=="Disconnected")return Promise.reject(new Error("Cannot start an HttpConnection that is not in the 'Disconnected' state."));if(this._connectionState="Connecting",this._startInternalPromise=this._startInternal(n),await this._startInternalPromise,this._connectionState==="Disconnecting"){let e="Failed to start the HttpConnection before stop() was called.";return this._logger.log(w.Error,e),await this._stopPromise,Promise.reject(new qt(e))}else if(this._connectionState!=="Connected"){let e="HttpConnection.startInternal completed gracefully but didn't enter the connection into the connected state!";return this._logger.log(w.Error,e),Promise.reject(new qt(e))}this._connectionStarted=!0}send(n){return this._connectionState!=="Connected"?Promise.reject(new Error("Cannot send data if the connection is not in the 'Connected' State.")):(this._sendQueue||(this._sendQueue=new My(this.transport)),this._sendQueue.send(n))}async stop(n){if(this._connectionState==="Disconnected")return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnected state.`),Promise.resolve();if(this._connectionState==="Disconnecting")return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnecting state.`),this._stopPromise;this._connectionState="Disconnecting",this._stopPromise=new Promise(e=>{this._stopPromiseResolver=e}),await this._stopInternal(n),await this._stopPromise}async _stopInternal(n){this._stopError=n;try{await this._startInternalPromise}catch{}if(this.transport){try{await this.transport.stop()}catch(e){this._logger.log(w.Error,`HttpConnection.transport.stop() threw error '${e}'.`),this._stopConnection()}this.transport=void 0}else this._logger.log(w.Debug,"HttpConnection.transport is undefined in HttpConnection.stop() because start() failed.")}async _startInternal(n){let e=this.baseUrl;this._accessTokenFactory=this._options.accessTokenFactory,this._httpClient._accessTokenFactory=this._accessTokenFactory;try{if(this._options.skipNegotiation)if(this._options.transport===Ut.WebSockets)this.transport=this._constructTransport(Ut.WebSockets),await this._startTransport(e,n);else throw new Error("Negotiation can only be skipped when using the WebSocket transport directly.");else{let i=null,r=0;do{if(i=await this._getNegotiationResponse(e),this._connectionState==="Disconnecting"||this._connectionState==="Disconnected")throw new qt("The connection was stopped during negotiation.");if(i.error)throw new Error(i.error);if(i.ProtocolVersion)throw new Error("Detected a connection attempt to an ASP.NET SignalR Server. This client only supports connecting to an ASP.NET Core SignalR Server. See https://aka.ms/signalr-core-differences for details.");if(i.url&&(e=i.url),i.accessToken){let o=i.accessToken;this._accessTokenFactory=()=>o,this._httpClient._accessToken=o,this._httpClient._accessTokenFactory=void 0}r++}while(i.url&&r<Nk);if(r===Nk&&i.url)throw new Error("Negotiate redirection limit exceeded.");await this._createTransport(e,this._options.transport,i,n)}this.transport instanceof hd&&(this.features.inherentKeepAlive=!0),this._connectionState==="Connecting"&&(this._logger.log(w.Debug,"The HttpConnection connected successfully."),this._connectionState="Connected")}catch(i){return this._logger.log(w.Error,"Failed to start the connection: "+i),this._connectionState="Disconnected",this.transport=void 0,this._stopPromiseResolver(),Promise.reject(i)}}async _getNegotiationResponse(n){let e={},[i,r]=cr();e[i]=r;let o=this._resolveNegotiateUrl(n);this._logger.log(w.Debug,`Sending negotiation request: ${o}.`);try{let s=await this._httpClient.post(o,{content:"",headers:b(b({},e),this._options.headers),timeout:this._options.timeout,withCredentials:this._options.withCredentials});if(s.statusCode!==200)return Promise.reject(new Error(`Unexpected status code returned from negotiate '${s.statusCode}'`));let a=JSON.parse(s.content);return(!a.negotiateVersion||a.negotiateVersion<1)&&(a.connectionToken=a.connectionId),a.useStatefulReconnect&&this._options._useStatefulReconnect!==!0?Promise.reject(new ud("Client didn't negotiate Stateful Reconnect but the server did.")):a}catch(s){let a="Failed to complete negotiation with the server: "+s;return s instanceof $n&&s.statusCode===404&&(a=a+" Either this is not a SignalR endpoint or there is a proxy blocking the connection."),this._logger.log(w.Error,a),Promise.reject(new ud(a))}}_createConnectUrl(n,e){return e?n+(n.indexOf("?")===-1?"?":"&")+`id=${e}`:n}async _createTransport(n,e,i,r){let o=this._createConnectUrl(n,i.connectionToken);if(this._isITransport(e)){this._logger.log(w.Debug,"Connection was provided an instance of ITransport, using that directly."),this.transport=e,await this._startTransport(o,r),this.connectionId=i.connectionId;return}let s=[],a=i.availableTransports||[],c=i;for(let l of a){let u=this._resolveTransportOrError(l,e,r,c?.useStatefulReconnect===!0);if(u instanceof Error)s.push(`${l.transport} failed:`),s.push(u);else if(this._isITransport(u)){if(this.transport=u,!c){try{c=await this._getNegotiationResponse(n)}catch(m){return Promise.reject(m)}o=this._createConnectUrl(n,c.connectionToken)}try{await this._startTransport(o,r),this.connectionId=c.connectionId;return}catch(m){if(this._logger.log(w.Error,`Failed to start the transport '${l.transport}': ${m}`),c=void 0,s.push(new Eh(`${l.transport} failed: ${m}`,Ut[l.transport])),this._connectionState!=="Connecting"){let g="Failed to select transport before stop() was called.";return this._logger.log(w.Debug,g),Promise.reject(new qt(g))}}}}return s.length>0?Promise.reject(new Sh(`Unable to connect to the server with any of the available transports. ${s.join(" ")}`,s)):Promise.reject(new Error("None of the transports supported by the client are supported by the server."))}_constructTransport(n){switch(n){case Ut.WebSockets:if(!this._options.WebSocket)throw new Error("'WebSocket' is not supported in your environment.");return new Vh(this._httpClient,this._accessTokenFactory,this._logger,this._options.logMessageContent,this._options.WebSocket,this._options.headers||{});case Ut.ServerSentEvents:if(!this._options.EventSource)throw new Error("'EventSource' is not supported in your environment.");return new Bh(this._httpClient,this._httpClient._accessToken,this._logger,this._options);case Ut.LongPolling:return new hd(this._httpClient,this._logger,this._options);default:throw new Error(`Unknown transport: ${n}.`)}}_startTransport(n,e){return this.transport.onreceive=this.onreceive,this.features.reconnect?this.transport.onclose=async i=>{let r=!1;if(this.features.reconnect)try{this.features.disconnected(),await this.transport.connect(n,e),await this.features.resend()}catch{r=!0}else{this._stopConnection(i);return}r&&this._stopConnection(i)}:this.transport.onclose=i=>this._stopConnection(i),this.transport.connect(n,e)}_resolveTransportOrError(n,e,i,r){let o=Ut[n.transport];if(o==null)return this._logger.log(w.Debug,`Skipping transport '${n.transport}' because it is not supported by this client.`),new Error(`Skipping transport '${n.transport}' because it is not supported by this client.`);if(TH(e,o))if(n.transferFormats.map(a=>Ot[a]).indexOf(i)>=0){if(o===Ut.WebSockets&&!this._options.WebSocket||o===Ut.ServerSentEvents&&!this._options.EventSource)return this._logger.log(w.Debug,`Skipping transport '${Ut[o]}' because it is not supported in your environment.'`),new xh(`'${Ut[o]}' is not supported in your environment.`,o);this._logger.log(w.Debug,`Selecting transport '${Ut[o]}'.`);try{return this.features.reconnect=o===Ut.WebSockets?r:void 0,this._constructTransport(o)}catch(a){return a}}else return this._logger.log(w.Debug,`Skipping transport '${Ut[o]}' because it does not support the requested transfer format '${Ot[i]}'.`),new Error(`'${Ut[o]}' does not support ${Ot[i]}.`);else return this._logger.log(w.Debug,`Skipping transport '${Ut[o]}' because it was disabled by the client.`),new Dh(`'${Ut[o]}' is disabled by the client.`,o)}_isITransport(n){return n&&typeof n=="object"&&"connect"in n}_stopConnection(n){if(this._logger.log(w.Debug,`HttpConnection.stopConnection(${n}) called while in state ${this._connectionState}.`),this.transport=void 0,n=this._stopError||n,this._stopError=void 0,this._connectionState==="Disconnected"){this._logger.log(w.Debug,`Call to HttpConnection.stopConnection(${n}) was ignored because the connection is already in the disconnected state.`);return}if(this._connectionState==="Connecting")throw this._logger.log(w.Warning,`Call to HttpConnection.stopConnection(${n}) was ignored because the connection is still in the connecting state.`),new Error(`HttpConnection.stopConnection(${n}) was called while the connection is still in the connecting state.`);if(this._connectionState==="Disconnecting"&&this._stopPromiseResolver(),n?this._logger.log(w.Error,`Connection disconnected with error '${n}'.`):this._logger.log(w.Information,"Connection disconnected."),this._sendQueue&&(this._sendQueue.stop().catch(e=>{this._logger.log(w.Error,`TransportSendQueue.stop() threw error '${e}'.`)}),this._sendQueue=void 0),this.connectionId=void 0,this._connectionState="Disconnected",this._connectionStarted){this._connectionStarted=!1;try{this.onclose&&this.onclose(n)}catch(e){this._logger.log(w.Error,`HttpConnection.onclose(${n}) threw error '${e}'.`)}}}_resolveUrl(n){if(n.lastIndexOf("https://",0)===0||n.lastIndexOf("http://",0)===0)return n;if(!Xe.isBrowser)throw new Error(`Cannot resolve '${n}'.`);let e=window.document.createElement("a");return e.href=n,this._logger.log(w.Information,`Normalizing '${n}' to '${e.href}'.`),e.href}_resolveNegotiateUrl(n){let e=new URL(n);e.pathname.endsWith("/")?e.pathname+="negotiate":e.pathname+="/negotiate";let i=new URLSearchParams(e.searchParams);return i.has("negotiateVersion")||i.append("negotiateVersion",this._negotiateVersion.toString()),i.has("useStatefulReconnect")?i.get("useStatefulReconnect")==="true"&&(this._options._useStatefulReconnect=!0):this._options._useStatefulReconnect===!0&&i.append("useStatefulReconnect","true"),e.search=i.toString(),e.toString()}};function TH(t,n){return!t||(n&t)!==0}var My=class t{constructor(n){this._transport=n,this._buffer=[],this._executing=!0,this._sendBufferedData=new rc,this._transportResult=new rc,this._sendLoopPromise=this._sendLoop()}send(n){return this._bufferData(n),this._transportResult||(this._transportResult=new rc),this._transportResult.promise}stop(){return this._executing=!1,this._sendBufferedData.resolve(),this._sendLoopPromise}_bufferData(n){if(this._buffer.length&&typeof this._buffer[0]!=typeof n)throw new Error(`Expected data to be of type ${typeof this._buffer} but was of type ${typeof n}`);this._buffer.push(n),this._sendBufferedData.resolve()}async _sendLoop(){for(;;){if(await this._sendBufferedData.promise,!this._executing){this._transportResult&&this._transportResult.reject("Connection stopped.");break}this._sendBufferedData=new rc;let n=this._transportResult;this._transportResult=void 0;let e=typeof this._buffer[0]=="string"?this._buffer.join(""):t._concatBuffers(this._buffer);this._buffer.length=0;try{await this._transport.send(e),n.resolve()}catch(i){n.reject(i)}}}static _concatBuffers(n){let e=n.map(o=>o.byteLength).reduce((o,s)=>o+s),i=new Uint8Array(e),r=0;for(let o of n)i.set(new Uint8Array(o),r),r+=o.byteLength;return i.buffer}},rc=class{constructor(){this.promise=new Promise((n,e)=>[this._resolver,this._rejecter]=[n,e])}resolve(){this._resolver()}reject(n){this._rejecter(n)}};var AH="json",Hh=class{constructor(){this.name=AH,this.version=2,this.transferFormat=Ot.Text}parseMessages(n,e){if(typeof n!="string")throw new Error("Invalid input for JSON hub protocol. Expected a string.");if(!n)return[];e===null&&(e=ar.instance);let i=Gn.parse(n),r=[];for(let o of i){let s=JSON.parse(o);if(typeof s.type!="number")throw new Error("Invalid payload.");switch(s.type){case ue.Invocation:this._isInvocationMessage(s);break;case ue.StreamItem:this._isStreamItemMessage(s);break;case ue.Completion:this._isCompletionMessage(s);break;case ue.Ping:break;case ue.Close:break;case ue.Ack:this._isAckMessage(s);break;case ue.Sequence:this._isSequenceMessage(s);break;default:e.log(w.Information,"Unknown message type '"+s.type+"' ignored.");continue}r.push(s)}return r}writeMessage(n){return Gn.write(JSON.stringify(n))}_isInvocationMessage(n){this._assertNotEmptyString(n.target,"Invalid payload for Invocation message."),n.invocationId!==void 0&&this._assertNotEmptyString(n.invocationId,"Invalid payload for Invocation message.")}_isStreamItemMessage(n){if(this._assertNotEmptyString(n.invocationId,"Invalid payload for StreamItem message."),n.item===void 0)throw new Error("Invalid payload for StreamItem message.")}_isCompletionMessage(n){if(n.result&&n.error)throw new Error("Invalid payload for Completion message.");!n.result&&n.error&&this._assertNotEmptyString(n.error,"Invalid payload for Completion message."),this._assertNotEmptyString(n.invocationId,"Invalid payload for Completion message.")}_isAckMessage(n){if(typeof n.sequenceId!="number")throw new Error("Invalid SequenceId for Ack message.")}_isSequenceMessage(n){if(typeof n.sequenceId!="number")throw new Error("Invalid SequenceId for Sequence message.")}_assertNotEmptyString(n,e){if(typeof n!="string"||n==="")throw new Error(e)}};var RH={trace:w.Trace,debug:w.Debug,info:w.Information,information:w.Information,warn:w.Warning,warning:w.Warning,error:w.Error,critical:w.Critical,none:w.None};function NH(t){let n=RH[t.toLowerCase()];if(typeof n<"u")return n;throw new Error(`Unknown log level: ${t}`)}var pd=class{configureLogging(n){if(Ye.isRequired(n,"logging"),OH(n))this.logger=n;else if(typeof n=="string"){let e=NH(n);this.logger=new Os(e)}else this.logger=new Os(n);return this}withUrl(n,e){return Ye.isRequired(n,"url"),Ye.isNotEmpty(n,"url"),this.url=n,typeof e=="object"?this.httpConnectionOptions=b(b({},this.httpConnectionOptions),e):this.httpConnectionOptions=Q(b({},this.httpConnectionOptions),{transport:e}),this}withHubProtocol(n){return Ye.isRequired(n,"protocol"),this.protocol=n,this}withAutomaticReconnect(n){if(this.reconnectPolicy)throw new Error("A reconnectPolicy has already been set.");return n?Array.isArray(n)?this.reconnectPolicy=new fd(n):this.reconnectPolicy=n:this.reconnectPolicy=new fd,this}withServerTimeout(n){return Ye.isRequired(n,"milliseconds"),this._serverTimeoutInMilliseconds=n,this}withKeepAliveInterval(n){return Ye.isRequired(n,"milliseconds"),this._keepAliveIntervalInMilliseconds=n,this}withStatefulReconnect(n){return this.httpConnectionOptions===void 0&&(this.httpConnectionOptions={}),this.httpConnectionOptions._useStatefulReconnect=!0,this._statefulReconnectBufferSize=n?.bufferSize,this}build(){let n=this.httpConnectionOptions||{};if(n.logger===void 0&&(n.logger=this.logger),!this.url)throw new Error("The 'HubConnectionBuilder.withUrl' method must be called before building the connection.");let e=new jh(this.url,n);return md.create(e,this.logger||ar.instance,this.protocol||new Hh,this.reconnectPolicy,this._serverTimeoutInMilliseconds,this._keepAliveIntervalInMilliseconds,this._statefulReconnectBufferSize)}};function OH(t){return t.log!==void 0}var ko=class t{hubUrl=Cn.hubUrl;hubConnection;orderSignal=V(null);createHubConnection(){this.hubConnection=new pd().withUrl(this.hubUrl,{withCredentials:!0}).withAutomaticReconnect().build(),this.hubConnection.start().catch(n=>console.log(n)),this.hubConnection.on("OrderCompleteNotification",n=>{this.orderSignal.set(n)})}stopHubConnection(){this.hubConnection?.state===Je.Connected&&this.hubConnection.stop().catch(n=>console.log(n))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})};var un=class t{baseUrl=Cn.apiUrl;http=d(an);signalrService=d(ko);currentUser=V(null);login(n){let e=new Bn;return e=e.append("useCookies",!0),this.http.post(this.baseUrl+"login",n,{params:e}).pipe(tt(i=>{i&&this.signalrService.createHubConnection()}))}register(n){return this.http.post(this.baseUrl+"account/register",n)}getUserInfo(){return this.http.get(this.baseUrl+"account/user-info").pipe(Y(n=>(this.currentUser.set(n),n)))}logout(){return this.http.post(this.baseUrl+"account/logout",{}).pipe(tt(()=>this.signalrService.stopHubConnection()))}updateAddress(n){return this.http.post(this.baseUrl+"account/address",n).pipe(tt(()=>{this.currentUser.update(e=>(e&&(e.address=n),e))}))}getAuthState(){return this.http.get(this.baseUrl+"account/auth-status")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};var Uh=class t{baseUrl=Cn.apiUrl;http=d(an);deliveryMethods=[];getDeliveryMethods(){return this.deliveryMethods.length>0?W(this.deliveryMethods):this.http.get(this.baseUrl+"payment/delivery-methods").pipe(Y(n=>(this.deliveryMethods=n.sort((e,i)=>i.price-e.price),n)))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};var PH=["input"],FH=["formField"],LH=["*"],zh=class{source;value;constructor(n,e){this.source=n,this.value=e}},BH={provide:ir,useExisting:Mt(()=>Ty),multi:!0},Ok=new y("MatRadioGroup"),VH=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})}),Ty=(()=>{class t{_changeDetector=d(ge);_value=null;_name=d(Te).getId("mat-radio-group-");_selected=null;_isInitialized=!1;_labelPosition="after";_disabled=!1;_required=!1;_buttonChanges;_controlValueAccessorChangeFn=()=>{};onTouched=()=>{};change=new U;_radios;color;get name(){return this._name}set name(e){this._name=e,this._updateRadioButtonNames()}get labelPosition(){return this._labelPosition}set labelPosition(e){this._labelPosition=e==="before"?"before":"after",this._markRadiosForCheck()}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this._updateSelectedRadioFromValue(),this._checkSelectedRadioButton())}_checkSelectedRadioButton(){this._selected&&!this._selected.checked&&(this._selected.checked=!0)}get selected(){return this._selected}set selected(e){this._selected=e,this.value=e?e.value:null,this._checkSelectedRadioButton()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markRadiosForCheck()}get required(){return this._required}set required(e){this._required=e,this._markRadiosForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markRadiosForCheck()}_disabledInteractive=!1;ngAfterContentInit(){this._isInitialized=!0,this._buttonChanges=this._radios.changes.subscribe(()=>{this.selected&&!this._radios.find(e=>e===this.selected)&&(this._selected=null)})}ngOnDestroy(){this._buttonChanges?.unsubscribe()}_touch(){this.onTouched&&this.onTouched()}_updateRadioButtonNames(){this._radios&&this._radios.forEach(e=>{e.name=this.name,e._markForCheck()})}_updateSelectedRadioFromValue(){let e=this._selected!==null&&this._selected.value===this._value;this._radios&&!e&&(this._selected=null,this._radios.forEach(i=>{i.checked=this.value===i.value,i.checked&&(this._selected=i)}))}_emitChangeEvent(){this._isInitialized&&this.change.emit(new zh(this._selected,this._value))}_markRadiosForCheck(){this._radios&&this._radios.forEach(e=>e._markForCheck())}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this.onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetector.markForCheck()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-radio-group"]],contentQueries:function(i,r,o){if(i&1&&yt(o,$h,5),i&2){let s;z(s=$())&&(r._radios=s)}},hostAttrs:["role","radiogroup",1,"mat-mdc-radio-group"],inputs:{color:"color",name:"name",labelPosition:"labelPosition",value:"value",selected:"selected",disabled:[2,"disabled","disabled",L],required:[2,"required","required",L],disabledInteractive:[2,"disabledInteractive","disabledInteractive",L]},outputs:{change:"change"},exportAs:["matRadioGroup"],features:[Oe([BH,{provide:Ok,useExisting:t}])]})}return t})(),$h=(()=>{class t{_elementRef=d(F);_changeDetector=d(ge);_focusMonitor=d(An);_radioDispatcher=d(qb);_defaultOptions=d(VH,{optional:!0});_ngZone=d(B);_renderer=d(we);_uniqueId=d(Te).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new U;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=Ae();_injector=d(q);constructor(){d(qe).load(Rn);let e=d(Ok,{optional:!0}),i=d(new ti("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=Et(i,0))}focus(e,i){i?this._focusMonitor.focusVia(this._inputElement,i,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,i)=>{e!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new zh(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,i;if(!e||!e.selected||this.disabled?i=this.tabIndex:i=e.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let r=this._inputElement?.nativeElement;r&&(r.setAttribute("tabindex",i+""),this._previousTabIndex=i,vt(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===r&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===r&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-radio-button"]],viewQuery:function(i,r){if(i&1&&je(PH,5)(FH,7,F),i&2){let o;z(o=$())&&(r._inputElement=o.first),z(o=$())&&(r._rippleTrigger=o.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(i,r){i&1&&A("focus",function(){return r._inputElement.nativeElement.focus()}),i&2&&(G("id",r.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),H("mat-primary",r.color==="primary")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("mat-mdc-radio-checked",r.checked)("mat-mdc-radio-disabled",r.disabled)("mat-mdc-radio-disabled-interactive",r.disabledInteractive)("_mat-animation-noopable",r._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",L],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Et(e)],checked:[2,"checked","checked",L],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",L],required:[2,"required","required",L],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",L]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:LH,decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition","for"],[1,"mdc-radio"],[1,"mat-mdc-radio-touch-target"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(i,r){i&1&&(ke(),f(0,"label",2,0)(2,"span",3),M(3,"span",4),f(4,"input",5,1),A("change",function(s){return r._onInputInteraction(s)}),h(),f(6,"span",6),M(7,"span",7)(8,"span",8),h(),f(9,"span",9),M(10,"span",10),h()(),f(11,"span",11),ie(12),h()()),i&2&&(x("labelPosition",r.labelPosition)("for",r.inputId),p(2),H("mdc-radio--disabled",r.disabled),p(2),x("id",r.inputId)("checked",r.checked)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),G("name",r.name)("value",r.value)("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),p(5),x("matRippleTrigger",r._rippleTrigger.nativeElement)("matRippleDisabled",r._isRippleDisabled())("matRippleCentered",!0))},dependencies:[er,wh],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  cursor: pointer;
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-hover-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-pressed-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--mat-radio-checked-ripple-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--mat-radio-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-radio-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-radio-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-radio-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-radio-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-radio-label-text-weight, var(--mat-sys-body-medium-weight));
  cursor: pointer;
}
.mat-mdc-radio-button .mdc-radio--disabled + .mat-internal-form-field-label {
  color: var(--mat-radio-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-radio-touch-target-size, 48px);
  width: var(--mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2})}return t})(),Pk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=de({type:t});static \u0275inj=ce({imports:[Ha,$h,Wt]})}return t})();var HH=(t,n)=>n.id;function UH(t,n){if(t&1&&(f(0,"label",2)(1,"mat-radio-button",3)(2,"div",4)(3,"strong"),v(4),xe(5,"currency"),h(),f(6,"span",5),v(7),h()()()()),t&2){let e=n.$implicit,i=E();p(),x("value",e)("checked",i.cartService.selectedDelivery()===e),p(3),Dm("",e.shortName," - ",Me(5,5,e.price)),p(3),X(e.description)}}var Gh=class t{checkoutService=d(Uh);cartService=d(rt);deliveryComplete=km();ngOnInit(){this.checkoutService.getDeliveryMethods().subscribe({next:n=>{if(this.cartService.cart()?.deliveryMethodId){let e=n.find(i=>i.id===this.cartService.cart()?.deliveryMethodId);e&&(this.cartService.selectedDelivery.set(e),this.deliveryComplete.emit(!0))}}})}async updateDeliveryMethod(n){this.cartService.selectedDelivery.set(n);let e=this.cartService.cart();e&&(e.deliveryMethodId=n.id,await this.cartService.setCart(e),this.deliveryComplete.emit(!0))}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-checkout-delivery"]],outputs:{deliveryComplete:"deliveryComplete"},decls:4,vars:1,consts:[[1,"w-full"],[1,"grid","grid-cols-2","gap-4",3,"change","value"],[1,"p-3","border","border-gray-200","cursor-pointer","w-full","h-full","hover:bg-purple-100"],[1,"w-full","h-full",3,"value","checked"],[1,"flex","flex-col","w-full","h-full"],[1,"text-sm"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"mat-radio-group",1),A("change",function(o){return i.updateDeliveryMethod(o.value)}),at(2,UH,8,7,"label",2,HH),h()()),e&2&&(p(),x("value",i.cartService.selectedDelivery()?.id),p(),ct(i.checkoutService.deliveryMethods))},dependencies:[Pk,Ty,$h,St],encapsulation:2})};var Mo=class t{transform(n){if(n&&"address"in n&&n.name){let{line1:e,line2:i,city:r,state:o,country:s,postal_code:a}=n?.address;return`${n.name}, ${e}${i?", "+i:""}, 
        ${r}, ${o}, ${a}, ${s}`}else if(n&&"line1"in n){let{line1:e,line2:i,city:r,state:o,country:s,postalCode:a}=n;return`${n.name}, ${e}${i?", "+i:""}, 
        ${r}, ${o}, ${a}, ${s}`}else return"Unknown address"}static \u0275fac=function(e){return new(e||t)};static \u0275pipe=no({name:"address",type:t,pure:!0})};var To=class t{transform(n){if(n&&"card"in n){let{brand:e,last4:i,exp_month:r,exp_year:o}=n.card;return`${e.toUpperCase()} **** **** **** ${i}, Exp: ${r}/${o}`}else if(n&&"last4"in n){let{brand:e,last4:i,expMonth:r,expYear:o}=n;return`${e.toUpperCase()} **** **** **** ${i}, Exp: ${r}/${o}`}else return"Unknown payment method"}static \u0275fac=function(e){return new(e||t)};static \u0275pipe=no({name:"paymentCard",type:t,pure:!0})};var zH=(t,n)=>n.productId;function $H(t,n){if(t&1&&(Ve(0,"tr")(1,"td",8)(2,"div",9),In(3,"img",10),Ve(4,"span"),v(5),Qe()()(),Ve(6,"td",11),v(7),Qe(),Ve(8,"td",12),v(9),xe(10,"currency"),Qe()()),t&2){let e=n.$implicit;p(3),ht("src",kn(e.pictureUrl),Xn),p(2),X(e.productName),p(2),oe("x",e.quantity),p(2),X(Me(10,5,e.price))}}var Wh=class t{cartService=d(rt);confirmationToken;static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-checkout-review"]],inputs:{confirmationToken:"confirmationToken"},decls:22,vars:6,consts:[[1,"mt-4","w-full"],[1,"text-lg","font-semibold"],[1,"font-medium"],[1,"mt-1","text-gray-500","flex"],[1,"mt-6","mx-auto"],[1,"border-b","border-gray-200"],[1,"w-full","text-center"],[1,"divide-y","divide-gray-200"],[1,"py-4"],[1,"flex","items-center","gap-4"],["alt","product image",1,"w-10","h-10",3,"src"],[1,"p-4"],[1,"p-4","text-right"]],template:function(e,i){e&1&&(Ve(0,"div",0)(1,"h4",1),v(2,"Billing and delivery information"),Qe(),Ve(3,"dl")(4,"dt",2),v(5,"Shipping address"),Qe(),Ve(6,"dd",3)(7,"span"),v(8),xe(9,"address"),Qe()(),Ve(10,"dt",2),v(11,"Payment details"),Qe(),Ve(12,"dd",3)(13,"span"),v(14),xe(15,"paymentCard"),Qe()()()(),Ve(16,"div",4)(17,"div",5)(18,"table",6)(19,"tbody",7),at(20,$H,11,7,"tr",null,zH),Qe()()()()),e&2&&(p(8),X(Me(9,2,i.confirmationToken?.shipping)),p(6),X(Me(15,4,i.confirmationToken?.payment_method_preview)),p(6),ct(i.cartService.cart()?.cartItems))},dependencies:[St,Mo,To],encapsulation:2})};var GH=["determinateSpinner"];function WH(t,n){if(t&1&&($t(),f(0,"svg",11),M(1,"circle",12),h()),t&2){let e=E();G("viewBox",e._viewBox()),p(),wr("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),G("r",e._circleRadius())}}var qH=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:Fk})}),Fk=100,YH=10,qh=(()=>{class t{_elementRef=d(F);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=d(qH),i=Ab(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=Fk;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-YH)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&je(GH,5),i&2){let o;z(o=$())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(G("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),jt("mat-"+r.color),wr("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),H("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",Et],diameter:[2,"diameter","diameter",Et],strokeWidth:[2,"strokeWidth","strokeWidth",Et]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(Ge(0,WH,2,8,"ng-template",null,0,ei),f(2,"div",2,1),$t(),f(4,"svg",3),M(5,"circle",4),h()(),$i(),f(6,"div",5)(7,"div",6)(8,"div",7),on(9,8),h(),f(10,"div",9),on(11,8),h(),f(12,"div",10),on(13,8),h()()()),i&2){let o=He(1);p(4),G("viewBox",r._viewBox()),p(),wr("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),G("r",r._circleRadius()),p(4),x("ngTemplateOutlet",o),p(2),x("ngTemplateOutlet",o),p(2),x("ngTemplateOutlet",o)}},dependencies:[Qi],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2})}return t})();function Qh(t){"@babel/helpers - typeof";return typeof Symbol=="function"&&typeof Symbol.iterator=="symbol"?Qh=function(n){return typeof n}:Qh=function(n){return n&&typeof Symbol=="function"&&n.constructor===Symbol&&n!==Symbol.prototype?"symbol":typeof n},Qh(t)}var Vk="dahlia",ZH=function(n){return n===3?"v3":n},jk="https://js.stripe.com",QH="".concat(jk,"/").concat(Vk,"/stripe.js"),KH=/^https:\/\/js\.stripe\.com\/v3\/?(\?.*)?$/,XH=/^https:\/\/js\.stripe\.com\/(v3|[a-z]+)\/stripe\.js(\?.*)?$/,Lk="loadStripe.setLoadParameters was called but an existing Stripe.js script already exists in the document; existing script parameters will be used",JH=function(n){return KH.test(n)||XH.test(n)},eU=function(){for(var n=document.querySelectorAll('script[src^="'.concat(jk,'"]')),e=0;e<n.length;e++){var i=n[e];if(JH(i.src))return i}return null},Bk=function(n){var e=n&&!n.advancedFraudSignals?"?advancedFraudSignals=false":"",i=document.createElement("script");i.src="".concat(QH).concat(e);var r=document.head||document.body;if(!r)throw new Error("Expected document.body not to be null. Stripe.js requires a <body> element.");return r.appendChild(i),i},tU=function(n,e){!n||!n._registerWrapper||n._registerWrapper({name:"stripe-js",version:"9.14.0",startTime:e})},gd=null,Yh=null,Zh=null,nU=function(n){return function(e){n(new Error("Failed to load Stripe.js",{cause:e}))}},iU=function(n,e){return function(){window.Stripe?n(window.Stripe):e(new Error("Stripe.js not available"))}},rU=function(n){return gd!==null?gd:(gd=new Promise(function(e,i){if(typeof window>"u"||typeof document>"u"){e(null);return}if(window.Stripe&&n&&console.warn(Lk),window.Stripe){e(window.Stripe);return}try{var r=eU();if(r&&n)console.warn(Lk);else if(!r)r=Bk(n);else if(r&&Zh!==null&&Yh!==null){var o;r.removeEventListener("load",Zh),r.removeEventListener("error",Yh),(o=r.parentNode)===null||o===void 0||o.removeChild(r),r=Bk(n)}Zh=iU(e,i),Yh=nU(i),r.addEventListener("load",Zh),r.addEventListener("error",Yh)}catch(s){i(s);return}}),gd.catch(function(e){return gd=null,Promise.reject(e)}))},oU=function(n,e,i){if(n===null)return null;var r=e[0];if(typeof r!="string")throw new Error("Expected publishable key to be of type string, got type ".concat(Qh(r)," instead."));var o=r.match(/^pk_test/),s=ZH(n.version),a=Vk;o&&s!==a&&console.warn("Stripe.js@".concat(s," was loaded on the page, but @stripe/stripe-js@").concat("9.14.0"," expected Stripe.js@").concat(a,". This may result in unexpected behavior. For more information, see https://docs.stripe.com/sdks/stripejs-versioning"));var c=n.apply(void 0,e);return tU(c,i),c},_d,Hk=!1,Uk=function(){return _d||(_d=rU(null).catch(function(n){return _d=null,Promise.reject(n)}),_d)};Promise.resolve().then(function(){return Uk()}).catch(function(t){Hk||console.warn(t)});var zk=function(){for(var n=arguments.length,e=new Array(n),i=0;i<n;i++)e[i]=arguments[i];Hk=!0;var r=Date.now();return Uk().then(function(o){return oU(o,e,r)})};var Kh=class t{baseUrl=Cn.apiUrl;stripePromise;cartService=d(rt);accountService=d(un);http=d(an);elements;addressElement;paymentElement;constructor(){this.stripePromise=zk(Cn.stripePublicKey)}getStripeInstance(){return this.stripePromise}async initializeElements(){if(!this.elements){let n=await this.getStripeInstance();if(n){let e=await Ho(this.createOrUpdatePaymentIntent());this.elements=n.elements({clientSecret:e.clientSecret,appearance:{labels:"floating"}})}else throw new Error("Stripe has not been loaded")}return this.elements}createOrUpdatePaymentIntent(){let n=this.cartService.cart(),e=!!n?.clientSecret;if(!n)throw new Error("Problem with cart");return this.http.post(this.baseUrl+"payment/"+n.id,{}).pipe(Y(async i=>(e||await this.cartService.setCart(i),i)))}async createConfirmationToken(){let n=await this.getStripeInstance(),e=await this.initializeElements(),i=await e.submit();if(i.error)throw new Error(i.error.message);if(n)return await n.createConfirmationToken({elements:e});throw new Error("Stripe not available")}async confirmPayment(n){let e=await this.getStripeInstance(),r=await(await this.initializeElements()).submit();if(r.error)throw new Error(r.error.message);let o=this.cartService.cart()?.clientSecret;if(e&&o)return await e.confirmPayment({clientSecret:o,confirmParams:{confirmation_token:n.id},redirect:"if_required"});throw new Error("Unable to load stripe")}async createPaymentElement(){if(!this.paymentElement){let n=await this.initializeElements();if(n)this.paymentElement=n.create("payment");else throw new Error("Elements instance has not been initialised")}return this.paymentElement}async createAddressElement(){if(!this.addressElement){let n=await this.initializeElements();if(n){let e=this.accountService.currentUser(),i={};e&&(i.name=e.firstName+" "+e.lastName),e?.address&&(i.address={line1:e.address.line1,line2:e.address.line2,city:e.address.city,state:e.address.state,country:e.address.country,postal_code:e.address.postalCode});let r={mode:"shipping",defaultValues:i};this.addressElement=n.create("address",r)}else throw new Error("Elements instance has not been loaded")}return this.addressElement}disposeElements(){this.elements=void 0,this.addressElement=void 0}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};var li=class t{baseUrl=Cn.apiUrl;http=d(an);orderComplete=!1;createOrder(n){return this.http.post(this.baseUrl+"orders",n)}getOrdersForUser(){return this.http.get(this.baseUrl+"orders")}getOrderDetailed(n){return this.http.get(this.baseUrl+"orders/"+n)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=J({token:t,factory:t.\u0275fac,providedIn:"root"})};function sU(t,n){t&1&&M(0,"mat-spinner",19)}function aU(t,n){if(t&1&&(f(0,"span"),v(1),xe(2,"currency"),h()),t&2){let e=E();p(),oe("Pay ",Me(2,1,e.cartService.totals()?.total))}}var Xh=class t{stripeService=d(Kh);addressElement;paymentElement;accountService=d(un);orderService=d(li);cartService=d(rt);snackbar=d(Eo);router=d(Ke);saveAddress=!1;completionStatus=V({address:!1,card:!1,delivery:!1});confirmationToken;loading=!1;async ngOnInit(){try{this.addressElement=await this.stripeService.createAddressElement(),this.addressElement.mount("#address-element"),this.addressElement.on("change",this.handleAddressChange),this.paymentElement=await this.stripeService.createPaymentElement(),this.paymentElement.mount("#payment-element"),this.paymentElement.on("change",this.handlePaymentChange)}catch(n){this.snackbar.error(n.message)}}handleAddressChange=n=>{this.completionStatus.update(e=>(e.address=n.complete,e))};handlePaymentChange=n=>{this.completionStatus.update(e=>(e.card=n.complete,e))};handleDeliveryChange(n){this.completionStatus.update(e=>(e.delivery=n,e))}onSaveAddressCheckboxChange(n){this.saveAddress=n.checked}async getConfirmationToken(){try{if(Object.values(this.completionStatus()).every(n=>n===!0)){let n=await this.stripeService.createConfirmationToken();if(n.error)throw new Error(n.error.message);this.confirmationToken=n.confirmationToken,console.log(this.confirmationToken)}}catch(n){this.snackbar.error(n.message)}}async onStepChange(n){if(n.selectedIndex===1&&this.saveAddress){let e=await this.getAddressFromStripeAddress();e&&Ho(this.accountService.updateAddress(e))}n.selectedIndex===2&&await Ho(this.stripeService.createOrUpdatePaymentIntent()),n.selectedIndex===3&&await this.getConfirmationToken()}async getAddressFromStripeAddress(){let n=await this.addressElement?.getValue(),e=n?.value.address;return e?{name:n.value.name,line1:e.line1,line2:e?.line2||void 0,city:e.city,state:e.state,country:e.country,postalCode:e.postal_code}:null}async confirmPayment(n){this.loading=!0;try{if(this.confirmationToken){let e=await this.stripeService.confirmPayment(this.confirmationToken);if(e.paymentIntent?.status==="succeeded"){let i=await this.createOrderModel();if(await Ho(this.orderService.createOrder(i)))this.orderService.orderComplete=!0,this.cartService.deleteCart(),this.cartService.selectedDelivery.set(null),this.router.navigateByUrl("/checkout/success");else throw new Error("Order creation failed")}else throw e.error?new Error(e.error.message):new Error("Something went wrong")}}catch(e){this.snackbar.error(e.message||"Something went wrong"),n.previous()}finally{this.loading=!1}}async createOrderModel(){let n=this.cartService.cart(),e=await this.getAddressFromStripeAddress(),i=this.confirmationToken?.payment_method_preview.card;if(!n?.id||!n.deliveryMethodId||!i||!e)throw new Error("Problem creating order");return{cartId:n.id,paymentSummary:{last4:+i.last4,brand:i.brand,expMonth:i.exp_month,expYear:i.exp_year},deliveryMethodId:n.deliveryMethodId,shippingAddress:e,discount:this.cartService.totals()?.discount}}ngOnDestroy(){this.stripeService.disposeElements()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-checkout"]],decls:38,vars:11,consts:[["stepper",""],[1,"flex","mt-12","gap-2"],[1,"w-3/4"],[1,"bg-white","border","border-gray-200","shadow-sm",3,"selectionChange","linear"],["label","Address",3,"completed"],["id","address-element"],[1,"flex","justify-end","mt-1"],[3,"change","checked"],[1,"flex","justify-between","mt-6"],["routerLink","/shop","mat-stroked-button","",1,"z-0"],["matStepperNext","","mat-flat-button","",1,"z-0",3,"disabled"],["label","Shipping",3,"completed"],[3,"deliveryComplete"],["matStepperPrevious","","mat-stroked-button",""],["label","Payment",3,"completed"],["id","payment-element"],["label","Confirmation"],[3,"confirmationToken"],["mat-flat-button","",1,"z-0",3,"click","disabled"],["diameter","20"],[1,"w-1/4"]],template:function(e,i){if(e&1){let r=lt();f(0,"div",1)(1,"div",2)(2,"mat-stepper",3,0),A("selectionChange",function(s){return i.onStepChange(s)}),f(4,"mat-step",4),M(5,"div",5),f(6,"div",6)(7,"mat-checkbox",7),A("change",function(s){return i.onSaveAddressCheckboxChange(s)}),v(8,"Save as default address"),h()(),f(9,"div",8)(10,"button",9),v(11,"Continue shopping"),h(),f(12,"button",10),v(13,"Next"),h()()(),f(14,"mat-step",11)(15,"app-checkout-delivery",12),A("deliveryComplete",function(s){return i.handleDeliveryChange(s)}),h(),f(16,"div",8)(17,"button",13),v(18,"Back"),h(),f(19,"button",10),v(20,"Next"),h()()(),f(21,"mat-step",14),M(22,"div",15),f(23,"div",8)(24,"button",13),v(25,"Back"),h(),f(26,"button",10),v(27,"Next"),h()()(),f(28,"mat-step",16),M(29,"app-checkout-review",17),f(30,"div",8)(31,"button",13),v(32,"Back"),h(),f(33,"button",18),A("click",function(){Ee(r);let s=He(3);return Se(i.confirmPayment(s))}),O(34,sU,1,0,"mat-spinner",19)(35,aU,3,3,"span"),h()()()()(),f(36,"div",20),M(37,"app-order-summary"),h()()}e&2&&(p(2),x("linear",!0),p(2),x("completed",i.completionStatus().address),p(3),x("checked",i.saveAddress),p(5),x("disabled",!i.completionStatus().address),p(2),x("completed",i.completionStatus().delivery),p(5),x("disabled",!i.completionStatus().delivery),p(2),x("completed",i.completionStatus().card),p(5),x("disabled",!i.completionStatus().card),p(3),x("confirmationToken",i.confirmationToken),p(4),x("disabled",!i.confirmationToken||i.loading),p(),P(i.loading?34:35))},dependencies:[ec,Ek,Cy,wy,xk,Dk,it,Nt,kk,Iy,qh,Gh,Wh,St],encapsulation:2})};var Jh=class t{fb=d(rh);accountService=d(un);router=d(Ke);activatedRoute=d(yn);returnUrl="/shop";constructor(){let n=this.activatedRoute.snapshot.queryParams.returnUrl;n&&(this.returnUrl=n)}loginForm=this.fb.group({email:[""],password:[""]});onSubmit(){this.accountService.login(this.loginForm.value).subscribe({next:()=>{this.accountService.getUserInfo().subscribe(),this.router.navigateByUrl(this.returnUrl)}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-login"]],decls:15,vars:1,consts:[[1,"max-w-lg","mx-auto","mt-32","p-8","bg-white"],[3,"ngSubmit","formGroup"],[1,"text-center","mb-6"],[1,"text-3xl","font-semibold","text-primary"],["appearance","outline",1,"w-full","mb-4"],["matInput","","formControlName","email","type","email","placeholder","name@example.com",2,"padding-left","15px !important"],["appearance","outline",1,"w-full","mb-6"],["matInput","","formControlName","password","type","password","placeholder","Password",2,"padding-left","15px !important"],["matButton","outlined","type","submit","color","primary",1,"w-full","py-2",2,"background-color","rgb(44, 97, 119)"]],template:function(e,i){e&1&&(f(0,"mat-card",0)(1,"form",1),A("ngSubmit",function(){return i.onSubmit()}),f(2,"div",2)(3,"h1",3),v(4,"Login"),h()(),f(5,"mat-form-field",4)(6,"mat-label"),v(7,"Email address"),h(),M(8,"input",5),nn(),h(),f(9,"mat-form-field",6)(10,"mat-label"),v(11,"Password"),h(),M(12,"input",7),nn(),h(),f(13,"button",8),v(14,"Sign in"),h()()()),e&2&&(p(),x("formGroup",i.loginForm),p(7),rn(),p(4),rn())},dependencies:[oh,yo,rr,or,bo,Nr,rd,uo,ci,Do,it,ai],encapsulation:2})};function cU(t,n){t&1&&(f(0,"mat-error"),v(1,"First Name is required "),h())}function lU(t,n){t&1&&(f(0,"mat-error"),v(1,"Last Name is required "),h())}function dU(t,n){t&1&&(f(0,"mat-error"),v(1,"Email is required "),h())}function uU(t,n){t&1&&(f(0,"mat-error"),v(1,"Email is invalid "),h())}function mU(t,n){t&1&&(f(0,"mat-error"),v(1,"Password is required "),h())}function fU(t,n){if(t&1&&(f(0,"li"),v(1),h()),t&2){let e=n.$implicit;p(),X(e)}}function hU(t,n){if(t&1&&(f(0,"div",10)(1,"ul",12),at(2,fU,2,1,"li",null,Cr),h()()),t&2){let e=E();p(2),ct(e.validationErrors)}}var ep=class t{fb=d(rh);accountService=d(un);router=d(Ke);snack=d(Eo);validationErrors=[];registerForm=this.fb.group({firstName:["",zn.required],lastName:["",zn.required],email:["",[zn.required,zn.email]],password:["",[zn.required]]});onSubmit(){this.accountService.register(this.registerForm.value).subscribe({next:()=>{this.snack.success("Registration successful - you can now login!"),this.router.navigateByUrl("/account/login")},error:n=>this.validationErrors=n})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-register"]],decls:29,vars:8,consts:[[1,"max-w-lg","mx-auto","mt-32","p-8","bg-white"],[3,"ngSubmit","formGroup"],[1,"text-center","mb-6"],[1,"text-3xl","font-semibold","text-primary"],["appearance","outline",1,"w-full","mb-4"],["matInput","","formControlName","firstName","type","text","placeholder","ROHIT",2,"padding-left","15px !important"],["appearance","outline",1,"w-full","mb-6"],["matInput","","formControlName","lastName","type","text","placeholder","goyal",2,"padding-left","15px !important"],["matInput","","formControlName","email","type","email","placeholder","name@example.com",2,"padding-left","15px !important"],["matInput","","formControlName","password","type","password","placeholder","Password",2,"padding-left","15px !important"],[1,"mb-3","p-4","bg-red-100","text-red-500"],["matButton","outlined","type","submit","color","primary",1,"w-full","py-2",3,"disabled"],[1,"list-disc","px-3"]],template:function(e,i){e&1&&(f(0,"mat-card",0)(1,"form",1),A("ngSubmit",function(){return i.onSubmit()}),f(2,"div",2)(3,"h1",3),v(4,"Register"),h()(),f(5,"mat-form-field",4)(6,"mat-label"),v(7,"First Name"),h(),M(8,"input",5),nn(),O(9,cU,2,0,"mat-error"),h(),f(10,"mat-form-field",6)(11,"mat-label"),v(12,"Last Name"),h(),M(13,"input",7),nn(),O(14,lU,2,0,"mat-error"),h(),f(15,"mat-form-field",4)(16,"mat-label"),v(17,"Email address"),h(),M(18,"input",8),nn(),O(19,dU,2,0,"mat-error"),O(20,uU,2,0,"mat-error"),h(),f(21,"mat-form-field",6)(22,"mat-label"),v(23,"Password"),h(),M(24,"input",9),nn(),O(25,mU,2,0,"mat-error"),h(),O(26,hU,4,0,"div",10),f(27,"button",11),v(28,"Register"),h()()()),e&2&&(p(),x("formGroup",i.registerForm),p(7),rn(),p(),P(i.registerForm.get("firstName")?.hasError("required")?9:-1),p(4),rn(),p(),P(i.registerForm.get("lastName")?.hasError("required")?14:-1),p(4),rn(),p(),P(i.registerForm.get("email")?.hasError("required")?19:-1),p(),P(i.registerForm.get("email")?.hasError("email")?20:-1),p(4),rn(),p(),P(i.registerForm.get("password")?.hasError("required")?25:-1),p(),P(i.validationErrors.length>0?26:-1),p(),x("disabled",i.registerForm.invalid))},dependencies:[oh,yo,rr,or,bo,Nr,rd,uo,ci,Do,it,ai,mh],encapsulation:2})};var vd=(t,n)=>{let e=d(un),i=d(Ke);return e.currentUser()?W(!0):e.getAuthState().pipe(Y(r=>r.isAuthenticated?!0:(i.navigate(["/account/login"],{queryParams:{returnUrl:n.url}}),!1)))};var $k=(t,n)=>{let e=d(rt),i=d(Ke),r=d(Eo);return!e.cart()||e.cart()?.cartItems.length===0?(r.error("Your cart is empty"),i.navigateByUrl("/cart"),!1):!0};function pU(t,n){if(t&1&&(f(0,"section",0)(1,"div",1)(2,"h2",2),v(3,"Thanks for your fake order!"),h(),f(4,"p",3),v(5,"Your order "),f(6,"span",4),v(7),h(),v(8," will never be processed as this is a fake shop. We will not notify you by email once your order has not been shipped."),h(),f(9,"div",5)(10,"dl",6)(11,"dt",7),v(12,"Date"),h(),f(13,"dd",8),v(14),xe(15,"date"),h()(),f(16,"dl",6)(17,"dt",7),v(18,"Payment method"),h(),f(19,"dd",8),v(20),xe(21,"paymentCard"),h()(),f(22,"dl",6)(23,"dt",7),v(24,"Address"),h(),f(25,"dd",8),v(26),xe(27,"address"),h()(),f(28,"dl",6)(29,"dt",7),v(30,"Amount"),h(),f(31,"dd",8),v(32),xe(33,"currency"),h()(),f(34,"dl",6)(35,"dt",7),v(36,"Status"),h(),f(37,"dd",8),v(38),h()()(),f(39,"div",9)(40,"button",10),v(41,"View your order"),h(),f(42,"button",11),v(43,"Continue shopping"),h()()()()),t&2){let e=n;p(7),oe("#",e.id),p(7),oe(" ",ms(15,8,e.orderDate,"medium")),p(6),oe(" ",Me(21,11,e.paymentSummary)),p(6),oe(" ",Me(27,13,e.shippingAddress)," "),p(6),X(Me(33,15,e.total)),p(6),X(e.status),p(2),x("routerLink",Jn("/order/",e.id))}}function gU(t,n){t&1&&(f(0,"section",0)(1,"div",1)(2,"h2",2),v(3,"Order processing, please wait"),h(),f(4,"div",5)(5,"div",12),M(6,"mat-spinner",13),f(7,"p",14),v(8,"Loading order..."),h(),f(9,"span"),v(10,"Your payment has been received, we are creating the order"),h()()(),f(11,"div",9)(12,"button",11),v(13,"Continue shopping"),h()()()())}var tp=class t{signalrService=d(ko);orderService=d(li);ngOnDestroy(){this.orderService.orderComplete=!1,this.signalrService.orderSignal.set(null)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-checkout-success"]],decls:2,vars:1,consts:[[1,"bg-white","py-16"],[1,"mx-auto","max-w-2xl","px-4"],[1,"font-semibold","text-2xl","mb-2"],[1,"text-gray-500","mb-8"],[1,"font-medium"],[1,"space-y-2","rounded-lg","border","border-gray-100","bg-gray-50","p-6","mb-8"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-normal","text-gray-500"],[1,"font-medium","text-gray-900","text-end"],[1,"flex","items-center","space-x-4"],["mat-flat-button","",3,"routerLink"],["routerLink","/shop","mat-stroked-button",""],[1,"flex","flex-col","justify-center","items-center"],["diameter","30"],[1,"text-xl"]],template:function(e,i){if(e&1&&O(0,pU,44,17,"section",0)(1,gU,14,0,"section",0),e&2){let r;P((r=i.signalrService.orderSignal())?0:1,r)}},dependencies:[it,Nt,qh,fs,St,Mo,To],encapsulation:2})};var _U=(t,n)=>n.id;function vU(t,n){if(t&1&&(f(0,"tr",10)(1,"th",11),v(2),h(),f(3,"td"),v(4),xe(5,"date"),h(),f(6,"td"),v(7),xe(8,"currency"),h(),f(9,"td"),v(10),h()()),t&2){let e=n.$implicit;x("routerLink",Jn("/order/",e.id)),p(2),oe("# ",e.id),p(2),X(ms(5,6,e.orderDate,"medium")),p(3),X(Me(8,9,e.total)),p(3),X(e.status)}}function bU(t,n){if(t&1&&at(0,vU,11,11,"tr",10,_U),t&2){let e=E();ct(e.orders)}}function yU(t,n){t&1&&(f(0,"tr")(1,"td",12),v(2,"No orders found."),h()())}var np=class t{orderService=d(li);orders=null;ngOnInit(){this.orderService.getOrdersForUser().subscribe({next:n=>{console.log("API returned:",n),this.orders=n},error:n=>{console.error(n)}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-order"]],decls:19,vars:1,consts:[[1,"mx-auto","mt-32"],[1,"font-semibold","text-2xl","mb-6","text-center"],[1,"flex","flex-col"],[1,"w-full"],[1,"min-w-full","divide-y","divide-gray-200","cursor-pointer"],[1,"bg-gray-50"],[1,"uppercase","text-gray-600","text-sm"],[1,"text-center","px-6","py-3"],[1,"text-left"],[1,"bg-white","divide-y","divide-gray-200"],[1,"hover:bg-gray-100",3,"routerLink"],[1,"px-6","py-3"],["colspan","4",1,"text-center","py-4"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"h2",1),v(2,"My orders"),h(),f(3,"div",2)(4,"div",3)(5,"table",4)(6,"thead",5)(7,"tr",6)(8,"th",7),v(9,"Order"),h(),f(10,"th",8),v(11,"Date"),h(),f(12,"th",8),v(13,"Total"),h(),f(14,"th",8),v(15,"Status"),h()()(),f(16,"tbody",9),O(17,bU,2,0)(18,yU,3,0,"tr"),h()()()()()),e&2&&(p(17),P(i.orders!==null?17:18))},dependencies:[Nt,St,fs],encapsulation:2})};var CU=(t,n)=>n.productId;function wU(t,n){if(t&1&&(f(0,"tr")(1,"td",22)(2,"div",23),M(3,"img",24),f(4,"span"),v(5),h()()(),f(6,"td",25),v(7),h(),f(8,"td",26),v(9),xe(10,"currency"),h()()),t&2){let e=n.$implicit;p(3),x("src",kn(e.pictureUrl),Xn),p(2),X(e.productName),p(2),oe("x",e.quantity),p(2),oe("",Me(10,5,e.price)," ")}}function xU(t,n){if(t&1){let e=lt();f(0,"mat-card",0)(1,"div",1)(2,"div",2)(3,"h2",3),v(4),h(),f(5,"button",4),A("click",function(){Ee(e);let r=E();return Se(r.onReturnClick())}),v(6),h()(),f(7,"div",5)(8,"div",6)(9,"h4",7),v(10,"Billing and delivery information"),h(),f(11,"dl")(12,"dt",8),v(13,"Shipping address"),h(),f(14,"dd",9),v(15),xe(16,"address"),h()(),f(17,"dl")(18,"dt",8),v(19,"Payment info"),h(),f(20,"dd",9),v(21),xe(22,"paymentCard"),h()()(),f(23,"div",6)(24,"h4",7),v(25,"Order details"),h(),f(26,"dl")(27,"dt",8),v(28,"Email address"),h(),f(29,"dd",9),v(30),h()(),f(31,"dl")(32,"dt",8),v(33,"Order status"),h(),f(34,"dd",9),v(35),h()(),f(36,"dl")(37,"dt",8),v(38,"Order date"),h(),f(39,"dd",9),v(40),xe(41,"date"),h()()()(),f(42,"div",10)(43,"div",11)(44,"table",12)(45,"tbody",13),at(46,wU,11,7,"tr",null,CU),h()()()(),f(48,"div",14)(49,"p",15),v(50,"Order summary"),h(),f(51,"div",16)(52,"div",6)(53,"dl",17)(54,"dt",18),v(55,"Subtotal"),h(),f(56,"dd",19),v(57),xe(58,"currency"),h()(),f(59,"dl",17)(60,"dt",18),v(61,"Discount"),h(),f(62,"dd",20),v(63),xe(64,"currency"),h()(),f(65,"dl",17)(66,"dt",18),v(67,"Delivery fee"),h(),f(68,"dd",19),v(69),xe(70,"currency"),h()()(),f(71,"dl",21)(72,"dt",18),v(73,"Total"),h(),f(74,"dd",19),v(75),xe(76,"currency"),h()()()()()()}if(t&2){let e=E();p(4),oe("Order summary for order #",e.order.id),p(2),X(e.buttonText),p(9),X(Me(16,11,e.order.shippingAddress)),p(6),X(Me(22,13,e.order.paymentSummary)),p(9),X(e.order.buyerEmail),p(5),X(e.order.status),p(5),X(ms(41,15,e.order.orderDate,"medium")),p(6),ct(e.order.orderItems),p(11),oe(" ",Me(58,18,e.order.subtotal)," "),p(6),oe(" -",Me(64,20,e.order.discount)," "),p(6),oe(" ",Me(70,22,e.order.shippingPrice)," "),p(6),oe(" ",Me(76,24,e.order.total)," ")}}var ip=class t{activatedRoute=d(yn);router=d(Ke);orderService=d(li);order;buttonText="Return to orders";ngOnInit(){this.loadOrder()}onReturnClick(){this.router.navigateByUrl("/orders")}loadOrder(){let n=this.activatedRoute.snapshot.paramMap.get("id");n&&this.orderService.getOrderDetailed(+n).subscribe({next:e=>this.order=e})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-order-detail"]],decls:1,vars:1,consts:[[1,"bg-white","py-8","max-w-5xl","mx-auto","shadow-md"],[1,"px-4","w-full"],[1,"flex","justify-between","items-center","align-middle"],[1,"text-2xl","text-center","font-semibold"],["mat-stroked-button","",3,"click"],[1,"mt-8","py-3","border-t","border-gray-200","flex","gap-16"],[1,"space-y-2"],[1,"text-lg","font-semibold"],[1,"font-medium"],[1,"mt-1","font-light"],[1,"mt-4"],[1,"border-y","border-gray-200"],[1,"w-full","text-center"],[1,"divide-y","divide-gray-200"],[1,"space-y-4","rounded-lg","border-t","border-gray-200","p-4","bg-white"],[1,"text-xl","font-semibold"],[1,"space-y-4"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium","text-gray-500"],[1,"font-medium","text-gray-900"],[1,"font-medium","text-green-500"],[1,"flex","items-center","justify-between","gap-4","border-t","border-gray-200","pt-2"],[1,"py-4"],[1,"flex","items-center","gap-4"],["alt","product image",1,"w-10","h-10",3,"src"],[1,"p-4"],[1,"p-4","text-right"]],template:function(e,i){e&1&&O(0,xU,77,26,"mat-card",0),e&2&&P(i.order?0:-1)},dependencies:[oS,uo,it,fs,Mo,To,St],encapsulation:2})};var Gk=(t,n)=>{let e=xd(li),i=xd(Ke);return e.orderComplete?!0:(i.navigateByUrl("/shop"),!1)};var Wk=[{path:"",component:gh},{path:"shop",component:ph},{path:"cart",component:yh},{path:"checkout",component:Xh,canActivate:[vd,$k]},{path:"checkout/success",component:tp,canActivate:[vd,Gk]},{path:"orders",component:np,canActivate:[vd]},{path:"order/:id",component:ip,canActivate:[vd]},{path:"shop/:id",component:_h},{path:"account/login",component:Jh},{path:"account/register",component:ep},{path:"**",redirectTo:"",pathMatch:"full"}];var rp=class t{cartService=d(rt);accService=d(un);signalrService=d(ko);init(){let n=localStorage.getItem("cart_id"),e=n?this.cartService.getCart(n):W(null);return Uo({cart:e,userInfo:this.accService.getUserInfo().pipe(tt(i=>{i&&this.signalrService.createHubConnection()}))})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})};var qk=(t,n)=>{let e=t.clone({withCredentials:!0});return n(e)};var Yk={providers:[kg(),yb(Wk),$v(Gv([qk])),gm(async()=>{let t=d(rp);return Sp(t.init()).finally(()=>{let n=document.getElementById("initial-splash");n&&n.remove()})})]};var DU=new y("MAT_BADGE_CONFIG"),Zk="mat-badge-content",EU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=D({type:t,selectors:[["ng-component"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-badge {
  position: relative;
}
.mat-badge.mat-badge {
  overflow: visible;
}

.mat-badge-content {
  position: absolute;
  text-align: center;
  display: inline-block;
  transition: transform 200ms ease-in-out;
  transform: scale(0.6);
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  box-sizing: border-box;
  pointer-events: none;
  background-color: var(--mat-badge-background-color, var(--mat-sys-error));
  color: var(--mat-badge-text-color, var(--mat-sys-on-error));
  font-family: var(--mat-badge-text-font, var(--mat-sys-label-small-font));
  font-weight: var(--mat-badge-text-weight, var(--mat-sys-label-small-weight));
  border-radius: var(--mat-badge-container-shape, var(--mat-sys-corner-full));
}
.mat-badge-above .mat-badge-content {
  bottom: 100%;
}
.mat-badge-below .mat-badge-content {
  top: 100%;
}
.mat-badge-before .mat-badge-content {
  right: 100%;
}
[dir=rtl] .mat-badge-before .mat-badge-content {
  right: auto;
  left: 100%;
}
.mat-badge-after .mat-badge-content {
  left: 100%;
}
[dir=rtl] .mat-badge-after .mat-badge-content {
  left: auto;
  right: 100%;
}
@media (forced-colors: active) {
  .mat-badge-content {
    outline: solid 1px;
    border-radius: 0;
  }
}

.mat-badge-disabled .mat-badge-content {
  background-color: var(--mat-badge-disabled-state-background-color, color-mix(in srgb, var(--mat-sys-error) 38%, transparent));
  color: var(--mat-badge-disabled-state-text-color, var(--mat-sys-on-error));
}

.mat-badge-hidden .mat-badge-content {
  display: none;
}

.ng-animate-disabled .mat-badge-content,
.mat-badge-content._mat-animation-noopable {
  transition: none;
}

.mat-badge-content.mat-badge-active {
  transform: none;
}

.mat-badge-small .mat-badge-content {
  width: var(--mat-badge-legacy-small-size-container-size, unset);
  height: var(--mat-badge-legacy-small-size-container-size, unset);
  min-width: var(--mat-badge-small-size-container-size, 6px);
  min-height: var(--mat-badge-small-size-container-size, 6px);
  line-height: var(--mat-badge-small-size-line-height, 6px);
  padding: var(--mat-badge-small-size-container-padding, 0);
  font-size: var(--mat-badge-small-size-text-size, 0);
  margin: var(--mat-badge-small-size-container-offset, -6px 0);
}
.mat-badge-small.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-small-size-container-overlap-offset, -6px);
}

.mat-badge-medium .mat-badge-content {
  width: var(--mat-badge-legacy-container-size, unset);
  height: var(--mat-badge-legacy-container-size, unset);
  min-width: var(--mat-badge-container-size, 16px);
  min-height: var(--mat-badge-container-size, 16px);
  line-height: var(--mat-badge-line-height, 16px);
  padding: var(--mat-badge-container-padding, 0 4px);
  font-size: var(--mat-badge-text-size, var(--mat-sys-label-small-size));
  margin: var(--mat-badge-container-offset, -12px 0);
}
.mat-badge-medium.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-container-overlap-offset, -12px);
}

.mat-badge-large .mat-badge-content {
  width: var(--mat-badge-legacy-large-size-container-size, unset);
  height: var(--mat-badge-legacy-large-size-container-size, unset);
  min-width: var(--mat-badge-large-size-container-size, 16px);
  min-height: var(--mat-badge-large-size-container-size, 16px);
  line-height: var(--mat-badge-large-size-line-height, 16px);
  padding: var(--mat-badge-large-size-container-padding, 0 4px);
  font-size: var(--mat-badge-large-size-text-size, var(--mat-sys-label-small-size));
  margin: var(--mat-badge-large-size-container-offset, -12px 0);
}
.mat-badge-large.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-large-size-container-overlap-offset, -12px);
}
`],encapsulation:2})}return t})(),Qk=(()=>{class t{_ngZone=d(B);_elementRef=d(F);_ariaDescriber=d(Tf);_renderer=d(we);_animationsDisabled=Ae();_idGenerator=d(Te);get color(){return this._color}set color(e){this._setColor(e),this._color=e}_color;overlap;disabled=!1;position;get content(){return this._content}set content(e){this._updateRenderedContent(e)}_content;get description(){return this._description}set description(e){this._updateDescription(e)}_description;size;hidden=!1;_badgeElement;_inlineBadgeDescription;_isInitialized=!1;_interactivityChecker=d(Ba);_document=d(Z);constructor(){let e=d(DU,{optional:!0}),i=d(qe);i.load(EU),i.load(Ji),this._color=e?.color||"primary",this.overlap=e?.overlap??!0,this.position=e?.position||"above after",this.size=e?.size||"medium"}isAbove(){return this.position.indexOf("below")===-1}isAfter(){return this.position.indexOf("before")===-1}getBadgeElement(){return this._badgeElement}ngOnInit(){this._clearExistingBadges(),this.content&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement(),this._updateRenderedContent(this.content)),this._isInitialized=!0}ngAfterViewInit(){}ngOnDestroy(){this._renderer.destroyNode&&(this._renderer.destroyNode(this._badgeElement),this._inlineBadgeDescription?.remove()),this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description)}_isHostInteractive(){return this._interactivityChecker.isFocusable(this._elementRef.nativeElement,{ignoreVisibility:!0})}_createBadgeElement(){let e=this._renderer.createElement("span"),i="mat-badge-active";return e.setAttribute("id",this._idGenerator.getId("mat-badge-content-")),e.setAttribute("aria-hidden","true"),e.classList.add(Zk),this._animationsDisabled&&e.classList.add("_mat-animation-noopable"),this._elementRef.nativeElement.appendChild(e),typeof requestAnimationFrame=="function"&&!this._animationsDisabled?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>{e.classList.add(i)})}):e.classList.add(i),e}_updateRenderedContent(e){let i=`${e??""}`.trim();this._isInitialized&&i&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement()),this._badgeElement&&(this._badgeElement.textContent=i),this._content=i}_updateDescription(e){this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description),(!e||this._isHostInteractive())&&this._removeInlineDescription(),this._description=e,this._isHostInteractive()?this._ariaDescriber.describe(this._elementRef.nativeElement,e):this._updateInlineDescription()}_updateInlineDescription(){this._inlineBadgeDescription||(this._inlineBadgeDescription=this._document.createElement("span"),this._inlineBadgeDescription.classList.add("cdk-visually-hidden")),this._inlineBadgeDescription.textContent=this.description,this._badgeElement?.appendChild(this._inlineBadgeDescription)}_removeInlineDescription(){this._inlineBadgeDescription?.remove(),this._inlineBadgeDescription=void 0}_setColor(e){let i=this._elementRef.nativeElement.classList;i.remove(`mat-badge-${this._color}`),e&&i.add(`mat-badge-${e}`)}_clearExistingBadges(){let e=this._elementRef.nativeElement.querySelectorAll(`:scope > .${Zk}`);for(let i of Array.from(e))i!==this._badgeElement&&i.remove()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matBadge",""]],hostAttrs:[1,"mat-badge"],hostVars:20,hostBindings:function(i,r){i&2&&H("mat-badge-overlap",r.overlap)("mat-badge-above",r.isAbove())("mat-badge-below",!r.isAbove())("mat-badge-before",!r.isAfter())("mat-badge-after",r.isAfter())("mat-badge-small",r.size==="small")("mat-badge-medium",r.size==="medium")("mat-badge-large",r.size==="large")("mat-badge-hidden",r.hidden||!r.content)("mat-badge-disabled",r.disabled)},inputs:{color:[0,"matBadgeColor","color"],overlap:[2,"matBadgeOverlap","overlap",L],disabled:[2,"matBadgeDisabled","disabled",L],position:[0,"matBadgePosition","position"],content:[0,"matBadge","content"],description:[0,"matBadgeDescription","description"],size:[0,"matBadgeSize","size"],hidden:[2,"matBadgeHidden","hidden",L]}})}return t})();var SU=()=>({exact:!0});function IU(t,n){if(t&1&&(f(0,"button",9)(1,"mat-icon"),v(2,"arrow_drop_down"),h(),f(3,"span"),v(4),h()()),t&2){let e=E(),i=He(17);x("matMenuTriggerFor",i),p(4),X(e.acctService.currentUser()?.email)}}function kU(t,n){t&1&&(f(0,"button",14),v(1,"Login"),h(),f(2,"button",15),v(3,"Register"),h())}var op=class t{cartService=d(rt);acctService=d(un);router=d(Ke);logOut(){this.acctService.logout().subscribe({next:()=>{this.acctService.currentUser.set(null),this.router.navigateByUrl("/")}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-header"]],decls:31,vars:5,consts:[["menu","matMenu"],[1,"shadow-md","p-3","w-full"],[1,"flex","align-middle","items-center","justify-between","max-w-screen-2xl","mx-auto"],["routerLink","/","src","images/footWearLogo.jpg","alt","app logo",1,"max-h-16",2,"object-fit","cover","border-radius","50%","height","100px","width","100px"],[1,"flex","gap-3","my-2","uppercase","text-xl"],["routerLink","/","routerLinkActive","active",3,"routerLinkActiveOptions"],["routerLink","/shop","routerLinkActive","active"],[1,"flex","gap-3","align-middle"],["routerLink","/cart","routerLinkActive","active","matBadgeSize","large",1,"custom-badge","mt-2","mr-2",3,"matBadge"],["mat-button","",3,"matMenuTriggerFor"],[1,"px-3"],["mat-menu-item","","routerLink","/cart"],["mat-menu-item","","routerLink","/orders"],["mat-menu-item","",3,"click"],["routerLink","/account/login","matButton","outlined",1,"custom-shadow-btn",2,"background-color","rgb(140, 156, 162)"],["routerLink","/account/register","matButton","outlined",1,"custom-shadow-btn",2,"background-color","rgb(140, 156, 162)"]],template:function(e,i){e&1&&(f(0,"header",1)(1,"div",2),M(2,"img",3),f(3,"nav",4)(4,"a",5),v(5,"Home"),h(),f(6,"a",6),v(7,"Shop"),h(),f(8,"a"),v(9,"Contact"),h()(),f(10,"div",7)(11,"a",8)(12,"mat-icon"),v(13,"add_shopping_cart"),h()(),O(14,IU,5,2,"button",9)(15,kU,4,0),h()()(),f(16,"mat-menu",10,0)(18,"button",11)(19,"mat-icon"),v(20,"shopping_cart"),h(),v(21," My cart "),h(),f(22,"button",12)(23,"mat-icon"),v(24,"history"),h(),v(25," View Orders "),h(),M(26,"mat-divider"),f(27,"button",13),A("click",function(){return i.logOut()}),f(28,"mat-icon"),v(29,"logout"),h(),v(30," Logout "),h()()),e&2&&(p(4),x("routerLinkActiveOptions",dv(4,SU)),p(7),x("matBadge",kn(i.cartService.itemCount())),p(3),P(i.acctService.currentUser()?14:15))},dependencies:[ln,it,Qk,Nt,bb,wo,ch,Rs,ad],styles:[".custom-badge[_ngcontent-%COMP%]   .mat-badge-content[_ngcontent-%COMP%]{width:24px;height:24px;font-size:18px;line-height:30px;font-weight:700}.custom-badge[_ngcontent-%COMP%]   .mat-icon[_ngcontent-%COMP%]{font-size:32px;width:32px;height:32px}a.active[_ngcontent-%COMP%]{color:#3f51b5}"]})};var sp=class t{title="Footwears";static \u0275fac=function(e){return new(e||t)};static \u0275cmp=D({type:t,selectors:[["app-root"]],decls:5,vars:1,consts:[[1,"container","mx-auto","mt-6"],[1,"text-3xl","font-bold","underline"]],template:function(e,i){e&1&&(M(0,"app-header"),f(1,"div",0)(2,"h2",1),v(3),h(),M(4,"router-outlet"),h()),e&2&&(p(3),oe("welcome to ",i.title))},dependencies:[op,Nl],encapsulation:2})};Pv(sp,Yk).catch(t=>console.error(t));
