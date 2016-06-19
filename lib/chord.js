const debug = require('debug')('chord'),
	SimplePeer = require('simple-peer'),
	events = require('events'),
	util = require('util'),
	co = require('co')

const Node = require('./node'),
	Hub = require('./hub')

const sleep = (time, ret) => new Promise((resolve, reject) => setTimeout(resolve, time, ret))
const timeout = (time, ret) => new Promise((resolve, reject) => setTimeout(reject, time, ret))

function waitOnce(emitter, message, time, error) {
	var done = false
	var wait = new Promise((resolve, reject) => {
		emitter.once(message,  data => !done && (done = true) && resolve(data))
	})
	return Promise.race([
		timeout(time || 30000, error || 'timeout waiting for ' + message + ' event'), wait
	])
}

function Chord(opts, bootstrap) {
	this.opts = Object.assign({

		peerOptions: { },
		nodeOptions: { },
		hubOptions: { },

		forwardTTL: 128,

		stabilizeInterval: 500,

		subscribePrefix: '-subscribe-prefix-',

		signalTimeout: 60000,
		waitEventTimeout: 1000,
		nodePollTimeout: 3000,

		maxJoinRetry: 10,

		// how many states we should check to tell if the network is stable
		nodeStateCheckLength: 30000/500,

		// once the network gets stable,
		// we will try to keep the connections between maxHubConnCount and minHubConnCount...
		maxHubConnCount: 10,
		minHubConnCount: 5,

		// ...by adjusting the finger size between minNodeFingerSize and maxNodeFingerSize
		minNodeFingerSize: 20,
		maxNodeFingerSize: 30,

	}, opts)

	this.node = new Node(Object.assign({
		id: this.opts.id,
	}, this.opts.nodeOptions), this)

	this.hub = new Hub(Object.assign({
		id: this.node.id,
	}, this.opts.hubOptions), this)

	this.id = this.node.id

	this.recvs = [ ]
	this.buffers = [ ]

	this.running = false

	this.subscriptions = { }

	events.EventEmitter.call(this)

	this.on('warning', err => this.debug('[WARN] ' + (err && err.message || err)))

	this.recvs.push((evt, data) => this.emit(evt, data) && false)

	bootstrap && setTimeout(_ => this.start(bootstrap))
}

util.inherits(Chord, events.EventEmitter)

Chord.prototype.debug = function(message) {
	var msg = '[%s] ' + message,
		args = [].slice.call(arguments, 1)
	debug.apply(null, [msg, this.id].concat(args))
}

// interface for node

Chord.prototype.pull = function *(id, method, arg) {
	return yield *this.hub.call(id, 'chord-node-pull', { method, arg })
}

// interface for hub

Chord.prototype.onrecv = function *(evt, data) {
	if (evt === 'hub-peer-error')
		return this.node.remove(data)
	else if (evt === 'hub-peer-close')
		return this.node.remove(data)
	else if (evt === 'hub-conn-error')
		return this.node.remove(data)
	else if (evt === 'chord-node-pull')
		return yield *this.node.pull(this.id, data.method, data.arg)
	else if (evt === 'chord-signal-start')
		return yield *this.connectViaProxy(data.id, data.proxyId, data.token)
	else if (evt === 'chord-forward')
		return this.forward(data.targets, data.evt, data.data, data.ttl)
	else if (evt === 'chord-subscript') {
		if (this.subscriptions[data.channel])
			this.subscriptions[data.channel][data.id] = data.time
	}
	else if (evt === 'chord-unsubscript') {
		if (this.subscriptions[data.channel])
			delete this.subscriptions[data.channel][data.id]
	}
	else
		this.recvs = this.recvs.filter(cb => !cb(evt, data))
}

Chord.prototype.connect = function *(id) {
	var conns = this.hub.conns,
		ids = Object.keys(conns),
		proxyId = ids.sort((a, b) => conns[a].uptime - conns[b].uptime)[0]
	if (proxyId) {
		var now = Date.now(),
			token = id + '>' + proxyId + '@' + now + '#' + Math.random()

		this.sendVia(proxyId, id, 'chord-signal-start', { id:this.id, proxyId, token })
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
	if (!(ttl -- > 0))
		throw 'ttl'

	var route = { /* nextId => dict(targetId) */ },
		runCo = gen => co(gen).catch(err => this.emit('warning', err))
	function regRoute(nextId, targetId) {
		var dict = route[nextId] || (route[nextId] = { })
		dict[targetId] = true
	}

	targets.forEach(targetId => {
		var nextId
		if (targetId === this.id || this.hub.has(targetId))
			regRoute(targetId, targetId)
		else if ((nextId = this.node._closestPrecedingFingerId(targetId)) !== this.id)
			regRoute(nextId, targetId)
	})

	Object.keys(route).map(nextId => {
		var targets
		if (nextId === this.id)
			runCo(this.onrecv(evt, data))
		else if ((targets = Object.keys(route[nextId])) && targets.length)
			runCo(this.hub.send(nextId, 'chord-forward', { targets, evt, data, ttl }))
	})
}

// connections

Chord.prototype.connectViaProxy = function *(id, proxyId, token, initiator) {
	this.sendVia(proxyId, id, 'chord-signal-ready-' + token)
	yield waitOnce(this, 'chord-signal-ready-' + token, this.opts.waitEventTimeout)

	this.debug('connecting to %s via node %s...', id, proxyId)

	var peer = new SimplePeer(Object.assign({ initiator }, this.opts.peerOptions))

	peer.on('signal', offer => {
		this.sendVia(proxyId, id, 'chord-signal-offer-' + token, offer)
	})

	this.recvs.push((evt, offer) => {
		evt === 'chord-signal-offer-' + token && peer.signal(offer)
		return this.hub.has(id)
	})

	yield waitOnce(peer, 'connect', this.opts.signalTimeout)
	this.hub.add(id, peer)

	return id
}

Chord.prototype.connectViaWebSocket = function *(sock, initiator) {
	sock.emit('id', this.id)
	var id = yield waitOnce(sock, 'id', this.opts.waitEventTimeout)

	this.debug('connecting to %s via websocket...', id)

	var peer = new SimplePeer(Object.assign({ initiator }, this.opts.peerOptions))
	peer.on('signal', data => sock.emit('signal', data))
	sock.on('signal', data => peer.signal(data))

	yield waitOnce(peer, 'connect', this.opts.signalTimeout)
	this.hub.add(id, peer)

	sock.disconnect()

	return id
}

Chord.prototype.connectLocal = function *(chord) {
	var peer1 = new SimplePeer(Object.assign({ initiator:true }, this.opts.peerOptions)),
		peer2 = new SimplePeer(chord.opts.peerOptions)

	peer1.on('signal', data => peer2.signal(data))
	peer2.on('signal', data => peer1.signal(data))

	yield Promise.all([
		waitOnce(peer1, 'connect', this.opts.signalTimeout),
		waitOnce(peer2, 'connect', this.opts.signalTimeout),
	])

	chord.hub.add(this.id, peer2)
	this.hub.add(chord.id, peer1)

	return chord.id
}

// helpers

Chord.prototype.adjustNodeFingerSize = function() {
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
			co(this.node.fixFinger(fingerIds.length))
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
			this.emit('warning', err)
			this.debug('join with %s failed, retrying(%d)', bootstrapId, retry)
			sleep(2000)
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
			yield waitOnce(sock, 'connect')
			bootstrapId = yield *this.connectViaWebSocket(sock, true)
		}

		if (bootstrapId && this.node.successorId === this.id)
			yield *this.joinOrRetry(bootstrapId)

		this.emit('chord-start')

		this.run()

	}.bind(this))
}

Chord.prototype.stop = function() {
	this.debug('stopping')
	this.running = false
	this.hub.destroy()
}

Chord.prototype.run = function() {
	if (!this.running) return co(function *() {
		this.running = true

		while (this.running) {
			yield sleep(this.opts.stabilizeInterval)

			try {
				yield Promise.race([
					Promise.all(this.node.poll().map(co)),
					sleep(this.opts.nodePollTimeout),
				])
			}
			catch (err) {
				this.emit('warning', err)
			}

			this.hub.checkCalls()
			this.adjustNodeFingerSize()
		}

		this.running = false
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
	return this.send(proxyId, 'chord-forward', { targets, evt, data, ttl })
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
		if (targets.length)
			this.send(targets, 'chord-subscript', { channel, id, time })
	}.bind(this))
}

Chord.prototype.unsubscribe = function(channel) {
	var key = this.opts.subscribePrefix + channel
	return co(function *() {
		var dict = yield *this.node.unsubscribe(key)
		delete this.subscriptions[channel]

		var targets = Object.keys(dict).filter(id => id !== this.id),
			id = this.id
		if (targets.length)
			this.send(targets, 'chord-unsubscript', { channel, id })
	}.bind(this))
}

Chord.prototype.publish = function(channel, evt, data) {
	var key = this.opts.subscribePrefix + channel
	return co(function *() {
		var dict = this.subscriptions[channel] || (yield *this.node.get(key)) || { },
			expiration = Date.now() - this.node.opts.subscriptionExpiration,
			targets = Object.keys(dict)
				.filter(id => id !== this.id && dict[id] > expiration)
		if (targets.length)
			this.send(targets, evt, data)
	}.bind(this))
}

module.exports = Chord