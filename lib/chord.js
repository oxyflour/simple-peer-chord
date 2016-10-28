const debug = require('debug')('chord'),
	SimplePeer = require('simple-peer'),
	events = require('events'),
	util = require('util'),
	co = require('co')

const Node = require('./node'),
	Hub = require('./hub')

const sleep = (time, ret) => new Promise((resolve, reject) => setTimeout(resolve, time, ret))
const timeout = (time, ret) => new Promise((resolve, reject) => setTimeout(reject, time, ret))
const once = (emitter, message, time, error) => new Promise((resolve, reject) => {
	setTimeout(reject, time || 30000, error || 'timeout waiting for ' + message + ' event')
	emitter.once(message, resolve)
})

function Chord(opts, bootstrap) {
	this.opts = Object.assign({

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
		minHubConnCount: 5,
		maxHubConnCount: 10,
		minNodeFingerSize: 16,
		maxNodeFingerSize: 128,

	}, opts)

	this.node = new Node(Object.assign({
		id: this.opts.id,
	}, this.opts.nodeOptions), this)

	this.hub = new Hub(Object.assign({
		id: this.node.id,
	}, this.opts.hubOptions), this)

	this.id = this.node.id

	this.subscriptions = { }

	events.EventEmitter.call(this)
	this.on('-hub-peer-error', id => this.node.remove(id))
	this.on('-hub-peer-close', id => this.node.remove(id))
	this.on('-hub-conn-error', id => this.node.remove(id))
	this.on('-chord-forward', data => this.forward(data.targets, data.evt, data.data, data.ttl))
	this.on('-chord-signal-start', data => co(this.connectViaProxy(data.id, data.proxyId, data.token)))
	this.on('-chord-node-notify', data => co(this.node.query(data.method, data.arg)))
	this.on('-chord-subscribe-update', data => {
		if (this.subscriptions[data.channel]) {
			if (data.time) {
				this.subscriptions[data.channel][data.id] = data.time
			}
			else {
				delete this.subscriptions[data.channel][data.id]
			}
		}
	})

	bootstrap && setTimeout(_ => this.start(bootstrap))
}

util.inherits(Chord, events.EventEmitter)

Chord.prototype.debug = function(message) {
	var msg = '[%s] ' + message,
		args = [].slice.call(arguments, 1)
	debug.apply(null, [msg, this.id].concat(args))
}

// interface for node

Chord.prototype.query = function *(id, method, arg) {
	return id === this.id ?
		yield *this.node.query(method, arg) :
		yield *this.hub.query(id, method, arg)
}

Chord.prototype.notify = function *(id, method, arg) {
	return id === this.id ?
		yield *this.node.query(method, arg) :
		yield *this.hub.send(id, '-chord-node-notify', { method, arg })
}

// interface for hub

Chord.prototype.connect = function *(id, conns) {
	var ids = Object.keys(conns),
		proxyId = ids.sort((a, b) => conns[a].uptime - conns[b].uptime)[0]
	if (proxyId) {
		var now = Date.now(),
			token = id + '>' + proxyId + '@' + now + '#' + Math.random()

		this.sendVia(proxyId, id, '-chord-signal-start', { id:this.id, proxyId, token })
		yield *this.connectViaProxy(id, proxyId, token, true)

		conns[id].uptime = Date.now()
		return conns[id]
	}
	else {
		throw 'nothing to connect'
	}
}

// routing

Chord.prototype.forward = function(targets, evt, data, ttl) {
	if (!(ttl -- > 0)) {
		throw 'ttl'
	}

	var route = { /* nextId => dict(targetId) */ },
		regRoute = (nextId, targetId) => (route[nextId] || (route[nextId] = { }))[targetId] = true
	targets.forEach(targetId => {
		var nextId
		if (targetId === this.id || this.hub.has(targetId)) {
			regRoute(targetId, targetId)
		}
		else if ((nextId = this.node._closestPrecedingFingerId(targetId)) !== this.id) {
			regRoute(nextId, targetId)
		}
	})

	Object.keys(route).map(nextId => {
		var targets
		if (nextId === this.id) {
			this.emit(evt, data)
		}
		else if ((targets = Object.keys(route[nextId])) && targets.length) {
			co(this.hub.send(nextId, '-chord-forward', { targets, evt, data, ttl }))
		}
	})
}

// connections

Chord.prototype.connectViaProxy = function *(id, proxyId, token, initiator) {
	this.sendVia(proxyId, id, '-chord-signal-ready-' + token)
	yield once(this, '-chord-signal-ready-' + token, this.opts.signalPrepareTimeout)

	this.debug('connecting to %s via node %s...', id, proxyId)

	var peer = new SimplePeer(Object.assign({ initiator }, this.opts.peerOptions)), handler
	peer.on('signal', offer => this.sendVia(proxyId, id, '-chord-signal-offer', { token, offer }))
	this.on('-chord-signal-offer', handler = data => data.token === token && peer.signal(data.offer))

	try {
		yield once(peer, 'connect', this.opts.signalTimeout)
		this.hub.add(id, peer)
		this.removeListener('-chord-signal-offer', handler)
	}
	catch (err) {
		peer.destroy()
		this.removeListener('-chord-signal-offer', handler)
		throw err
	}

	return id
}

Chord.prototype.connectViaSocketIO = function *(sock, initiator) {
	sock.emit('id', this.id)
	var id = yield once(sock, 'id', this.opts.signalPrepareTimeout)

	this.debug('connecting to %s via websocket...', id)

	var peer = new SimplePeer(Object.assign({ initiator }, this.opts.peerOptions))
	peer.on('signal', data => sock.emit('signal', data))
	sock.on('signal', data => peer.signal(data))

	try {
		yield once(peer, 'connect', this.opts.signalTimeout)
		this.hub.add(id, peer)
		sock.disconnect()
	}
	catch (err) {
		peer.destroy()
		sock.disconnect()
		throw err
	}

	return id
}

Chord.prototype.connectLocal = function *(chord) {
	var peer1 = new SimplePeer(Object.assign({ initiator:true }, this.opts.peerOptions)),
		peer2 = new SimplePeer(chord.opts.peerOptions)

	peer1.on('signal', data => peer2.signal(data))
	peer2.on('signal', data => peer1.signal(data))

	yield [
		once(peer1, 'connect', this.opts.signalTimeout),
		once(peer2, 'connect', this.opts.signalTimeout),
	]

	chord.hub.add(this.id, peer2)
	this.hub.add(chord.id, peer1)

	return chord.id
}

// helpers

Chord.prototype.adjustNodeFingerSize = function *() {
	this.lastNodeStates = this.lastNodeStates || [ ]

	var state = [this.node.fingerIds, this.node.predecessorId, this.node.succBackupIds].join(';')
	this.lastNodeStates = this.lastNodeStates.concat(state).slice(-this.opts.nodeStateCheckLength)

	var isStable =
		this.lastNodeStates.length === this.opts.nodeStateCheckLength &&
		this.lastNodeStates.every(state => state === this.lastNodeStates[0])
	if (isStable) {
		var connCount = Object.keys(this.hub.conns).length,
			fingerIds = this.node.fingerIds,
			maxFingerSize = Math.min(this.opts.maxNodeFingerSize, this.node.opts.fingerTableSize)
		if (connCount > this.opts.maxHubConnCount &&
				fingerIds.length > this.opts.minNodeFingerSize) {
			this.node.fingerIds.pop()
			this.lastNodeStates = [ ]
			this.debug('reducing finger size to %d', fingerIds.length)
		}
		else if (connCount < this.opts.minHubConnCount &&
				fingerIds.length < maxFingerSize) {
			yield *this.node.fixFinger(fingerIds.length)
			this.lastNodeStates = [ ]
			this.debug('increasing finger size to %d', fingerIds.length)
		}
	}
}

Chord.prototype.joinOrRetry = function *(bootstrapId) {
	var retry = this.opts.maxJoinRetry
	while (retry -- > 0) {
		try {
			yield *this.node.join(bootstrapId)
			this.debug('successfully joined %s', bootstrapId)
			return
		}
		catch (err) {
			this.emit('chord-join-error', err)
			this.debug('join with %s failed, retrying(%d)\n%s',
				bootstrapId, retry, err && (err.stack || err.message || err))
			yield sleep(2000)
		}
	}
	this.running = false
	this.debug('failed to joined %s', bootstrapId)
	throw 'join failed'
}

// api

Chord.prototype.start = function(bootstrap) {
	return co(function *() {
		var bootstrapId
		if (bootstrap instanceof Chord) {
			bootstrapId = yield *this.connectLocal(bootstrap)
		}
		else if (bootstrap && bootstrap.url) {
			var sock = io(bootstrap.url, bootstrap.opts)
			yield once(sock, 'connect')
			bootstrapId = yield *this.connectViaSocketIO(sock, true)
		}

		if (bootstrapId) {
			yield *this.joinOrRetry(bootstrapId)
		}

		if (!this.isRunning) {
			this.emit('chord-start')
			this.run()
		}

	}.bind(this))
}

Chord.prototype.stop = function() {
	this.debug('stopping')
	this.hub.destroy()

	if (this.isRunning) {
		this.emit('chord-stop')
		this.isRunning = false
	}
}

Chord.prototype.run = function() {
	return co(function *() {
		var isRunning = this.isRunning = 'token-' + Math.random(),
			checkTimeout = setInterval(_ => this.hub.checkTimeout(), this.opts.stabilizeInterval)
		while (this.isRunning === isRunning) {
			try {
				yield *this.node.poll()
				yield *this.adjustNodeFingerSize()
			}
			catch (err) {
				this.emit('chord-run-error', err)
				console.error('[ERR from ' + this.id + ']', err, err && err.stack || '')
			}
			yield sleep(this.opts.stabilizeInterval)
		}
		clearInterval(checkTimeout)
	}.bind(this))
}

Chord.prototype.send = function(targets, evt, data, ttl) {
	ttl = ttl !== undefined ? ttl : this.opts.forwardTTL
	targets = Array.isArray(targets) ? targets : [ targets ]
	return this.forward(targets, evt, data, ttl)
}

Chord.prototype.sendVia = function(proxyId, targets, evt, data, ttl) {
	ttl = ttl !== undefined ? ttl : this.opts.forwardTTL
	targets = Array.isArray(targets) ? targets : [ targets ]
	return this.send(proxyId, '-chord-forward', { targets, evt, data, ttl })
}

Chord.prototype.ping = function(id, evt, data) {
	return this.sendVia(id, this.id, evt, data)
}

Chord.prototype.put = function(key, value) {
	return co(this.node.put(key, value))
}

Chord.prototype.get = function(key) {
	return co(this.node.get(key))
}

Chord.prototype.subscribe = function(channel) {
	var key = this.opts.subscribePrefix + channel
	return co(function *() {
		var dict = yield *this.node.subscribe(key)
		this.subscriptions[channel] = dict

		var targets = Object.keys(dict).filter(id => id !== this.id),
			id = this.id, time = dict[id]
		if (targets.length) {
			this.send(targets, '-chord-subscribe-update', { channel, id, time })
		}
	}.bind(this))
}

Chord.prototype.unsubscribe = function(channel) {
	var key = this.opts.subscribePrefix + channel
	return co(function *() {
		var dict = yield *this.node.unsubscribe(key)
		delete this.subscriptions[channel]

		var targets = Object.keys(dict).filter(id => id !== this.id),
			id = this.id
		if (targets.length) {
			this.send(targets, '-chord-subscribe-update', { channel, id })
		}
	}.bind(this))
}

Chord.prototype.publish = function(channel, evt, data) {
	var key = this.opts.subscribePrefix + channel
	return co(function *() {
		var dict = this.subscriptions[channel] || (yield *this.node.get(key)) || { },
			expiration = Date.now() - this.node.opts.subscriptionExpiration,
			targets = Object.keys(dict)
				.filter(id => id !== this.id && dict[id] > expiration)
		if (targets.length) {
			this.send(targets, evt, data)
		}
	}.bind(this))
}

module.exports = Chord