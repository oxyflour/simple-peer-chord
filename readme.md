# simple-peer-chord
An experimental Chord implement for decentralized, serverless applications on browsers

WARNING: This library is merely a prove of concept and does not consider much about performance or security. It has only been tested on small networks containing a few tens of nodes.

![simple-peer-chord](https://cloud.githubusercontent.com/assets/1967664/19626428/57e644f6-9964-11e6-8743-4d24e6035a3f.png)

## Features
* configurable connection numbers and dynamic finger length
* decentralized signaling
* simple distributed, redundant storage
* simple multicasting
* publish/subscribe messages to arbitrary channels

## API (experimental)
### ```var chord = new Chord(opts, [bootstrap])```
Create a new Chord network. The ```opts``` may contain the following fields
```javascript
{
  // options to pass to simple-peer
  peerOptions: { },
  // options to pass to ./lib/node.js
  nodeOptions: { },
  // options to pass to ./lib/hub.js
  hubOptions: { },

  // max hops forwarding data in the network
  forwardTTL: 128,

  //
  stabilizeInterval: 500,

  // use your public key here to avoid conflicts with other networks
  subscribePrefix: '-subscribe-prefix-',

  // how long to wait before the two peers finish exchanging data
  signalPrepareTimeout: 5000,
  // how long we should wait before `connect` events fired from simple-peer
  signalTimeout: 60000,

  // 
  maxJoinRetry: 10,

  // how many states we should check to tell if the network is stable
  nodeStateCheckLength: 30000/500,

  // once the network gets stable,
  // it will try to keep the connection number between maxHubConnCount and minHubConnCount
  // by adjusting the finger size between minNodeFingerSize and maxNodeFingerSize
  maxHubConnCount: 10,
  minHubConnCount: 5,
  minNodeFingerSize: 20,
  maxNodeFingerSize: 30,
}
```
And the ```bootstrap``` may be another Chord instance or an object containing URL of the signaler server. ```chord.start``` will be called if ```bootstrap``` argument is given.

### ```chord.id```
Id of this node. It's a plain hex string at present.

### ```chord.on(event, listener)```
### ```chord.once(event, listener)```
The Chord class inherits from Event class so you can attach event listeners to receive messages from network. Events starting with ```-``` are used internally. Alway add prefix with your custom events.

### ```chord.start([bootstrap])```
### ```chord.stop()```
```chord.start``` will be automatically called if bootstrap argument is given in the constructor.

### ```chord.send(targets, event, data, [ttl])```
### ```chord.sendVia(proxyId, targets, event, data, [ttl])```
```targets``` may be a single peer id or an array of them. The events will be routed only once even for multiple targets. Specially, you can ask a third node (by ```proxyId```) to forward events for you. That's the key part of how the decentralized signaling is implemented (see below). Alway add prefix with your custom events.

### ```chord.get(key)```
### ```chord.put(key, value)```
Fetch/save data in the dht network. Promises are returned.

### ```chord.subscribe(channel)```
### ```chord.unsubscribe(channel)```
Subscribe/Unsubscribe to a channel. The subscribers' ids are saved in the DHT network, so it may take some time before you start/stop receiving events from that channel. You should call ```chord.subscribe``` periodically or the network will forget your subscriptions.

### ```chord.publish(channel, event, data)```
Publish events to a channel. Alway add prefix with your custom events.

## Decentralized Signaling (experimental)
The ```signaler.js``` script in the library runs in node.js and can be used as a bootstrap node for decentralized signaling. The bootstraps node will:
* accept connections from new nodes via websocket
* proxy signaling data between the new node and other nodes (via ```chord.sendVia```) to help the new node joining the network
* exchange URLs of other bootstrap nodes in a certain channel (via ```chord.subscribe``` and ```chord.publish```)
* periodically connect to other bootstrap nodes via websocket and check which one knows more nodes. The one knowing less nodes will try to rejoin to the opposite side.

## Examples
See this [example script](https://github.com/oxyflour/simple-peer-chord/blob/master/example/index.js) or the [simple-peer-chord-demo repo](https://github.com/oxyflour/simple-peer-chord-demo).