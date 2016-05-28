const debug = require('debug')('chord'),
	SimplePeer = require('simple-peer'),
	co = require('co')

const Node = require('./node'),
	Hub = require('./hub')

const sleep = (time, ret) => new Promise((resolve, reject) => setTimeout(resolve, time, ret))
const timeout = (time, ret) => new Promise((resolve, reject) => setTimeout(reject, time, ret))

function waitOnce(emitter, message, time, error) {
	var done = false
	var wait = new Promise((resolve, reject) => {
		emitter.once(message,  data => !done && (done = true) && resolve(data))
		emitter.once('error',  err  => !done && (done = true) && reject(err))
	})
	return Promise.race([
		timeout(time || 30000, error || 'timeout waiting for ' + message + ' event'),
		wait
	])
}

function Chord(opts) {
	this.opts = Object.assign({

		peerOptions: { },
		nodeOptions: { },
		hubOptions: { },

		forwardTTL: 128,

		stabilizeInterval: 500,

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

	this.recvs = [ ]
	this.buffers = [ ]

	this.started = false
	this.sending = false

	this.id = this.node.id
}

// interface for node

Chord.prototype.pull = function *(id, method, arg) {
	try {
		return yield *this.hub.call(id, 'node-pull', { method, arg })
	}
	catch (err) {
		debug('[%s] node %s seems dead', this.id, id)
		this.node.remove(id)
		this.hub.remove(id)
		throw err
	}
}

// interface for hub

Chord.prototype.onrecv = function *(evt, data) {
	if (evt === 'peer-error')
		this.node.remove(data)
	else if (evt === 'peer-close')
		this.node.remove(data)
	else if (evt === 'node-pull')
		return yield *this.node.pull(this.id, data.method, data.arg)
	else if (evt === 'signal-start')
		co(this.acceptViaProxy(data.id, data.proxyId, data.token))
	else if (evt === 'forward')
		return this.send(data.id, data.evt, data.data, data.ttl)
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

		this.sendVia(proxyId, id, 'signal-start', { id:this.id, proxyId, token })
		yield *this.connectViaProxy(id, proxyId, token)

		conns[id].uptime = Date.now()
		return conns[id]
	}
	else {
		this.started = false
		debug('[%s] stopped', this.id)
		throw 'nothing to connect. stopping...'
	}
}

// routing

Chord.prototype.forward = function *(id, evt, data, ttl) {
	var nextId
	if (!(ttl-- > 0)) {
		console.warn('[' + this.id + ']', 'package to ' + id + ' ignored')
	}
	else if (id === this.id) {
		yield *this.onrecv(evt, data)
	}
	else if (this.hub.has(id)) {
		debug('[%s] forwarding "%s" to %s, ttl %s', this.id, evt, id, ttl)
		yield *this.hub.send(id, evt, data)
	}
	else if ((nextId = this.node._closestPrecedingFingerId(id)) === this.id) {
		throw 'no route to ' + id
	}
	else {
		debug('[%s] forwarding "%s" to %s via %s, ttl %s', this.id, evt, id, nextId, ttl)
		yield *this.hub.send(nextId, 'forward', { id, evt, data, ttl })
	}
}

Chord.prototype.waitEvent = function *(test, time, message) {
	yield Promise.race([
		timeout(time || 1000, message || 'wait timeout'),
		new Promise(resolve => this.recv((evt, data) => {
			if (test(evt, data)) {
				resolve({ evt, data })
				return true
			}
		}))
	])
}

// connections

Chord.prototype.connectViaProxy = function *(id, proxyId, token) {
	yield *this.waitEvent((evt, data) => evt === 'signal-ready' && data.token === token,
		this.opts.waitEventTimeout)

	debug('[%s] connecting to %s via node %s...', this.id, id, proxyId)

	var peer = new SimplePeer(Object.assign({ initiator:true }, this.opts.peerOptions))

	peer.on('signal', offer => {
		this.sendVia(proxyId, id, 'signal-offer', { offer, token })
	})

	this.recv((evt, data) => {
		if (evt === 'signal-answer' && data.token === token)
			peer.signal(data.answer)
		return this.hub.has(id)
	})

	yield waitOnce(peer, 'connect', this.opts.signalTimeout)
	this.hub.add(id, peer)
}

Chord.prototype.acceptViaProxy = function *(id, proxyId, token) {
	this.sendVia(proxyId, id, 'signal-ready', { token })

	debug('[%s] accepting %s via node %s...', this.id, id, proxyId)

	var peer = new SimplePeer(this.opts.peerOptions)

	this.recv((evt, data) => {
		if (evt === 'signal-offer' && data.token === token)
			peer.signal(data.offer)
		return this.hub.has(id)
	})

	peer.on('signal', answer => {
		this.sendVia(proxyId, id, 'signal-answer', { answer, token })
	})

	yield waitOnce(peer, 'connect', this.opts.signalTimeout)
	this.hub.add(id, peer)
}

Chord.prototype.connectWebsocket = function *(sock) {
	sock.emit('id', this.id)
	var id = yield waitOnce(sock, 'id', this.opts.signalTimeout)

	debug('[%s] connecting to %s via websocket...', this.id, id)

	var peer = new SimplePeer(Object.assign({ initiator:true }, this.opts.peerOptions))
	peer.on('signal', data => sock.emit('signal', data))
	sock.on('signal', data => peer.signal(data))

	yield waitOnce(peer, 'connect', this.opts.signalTimeout)
	this.hub.add(id, peer)
	sock.disconnect()
	return id
}

Chord.prototype.acceptWebsocket = function *(sock) {
	var id = yield waitOnce(sock, 'id', this.opts.signalTimeout)
	sock.emit('id', this.id)

	debug('[%s] accepting %s via websocket...', this.id, id)

	var peer = new SimplePeer(this.opts.peerOptions)
	sock.on('signal', data => peer.signal(data))
	peer.on('signal', data => sock.emit('signal', data))

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
	this.hub.add(chord.node.id, peer1)

	return chord.node.id
}

// helpers

Chord.prototype.adjustNodeFingerSize = function *() {
	this.lastNodeStates = this.lastNodeStates || [ ]

	var state = [this.fingerIds, this.predecessorId, this.succBackupIds].join(';')
	this.lastNodeStates = this.lastNodeStates.concat(state).slice(-this.opts.nodeStateCheckLength)

	var isStable =
		this.lastNodeStates.length === this.opts.nodeStateCheckLength &&
		this.lastNodeStates.every(state => state === this.lastNodeStates[0])
	if (isStable) {
		var connCount = Object.keys(this.hub.conns).length,
			fingerIds = this.node.fingerIds
		if (connCount > this.opts.maxHubConnCount &&
				fingerIds.length > this.opts.minNodeFingerSize) {
			this.node.fingerIds.pop()
			this.lastNodeStates = [ ]
			debug('[%s] reducing finger size to %d', this.id, fingerIds.length)
		}
		else if (connCount < this.opts.minHubConnCount &&
				fingerIds.length < Math.min(this.opts.maxNodeFingerSize, this.node.opts.fingerTableSize)) {
			yield *this.node.fixFinger(fingerIds.length)
			this.lastNodeStates = [ ]
			debug('[%s] increasing finger size to %d', this.id, fingerIds.length)
		}
	}
}

Chord.prototype.join = function *(bootstrapId) {
	var retry = this.opts.maxJoinRetry
	while (retry -- > 0) {
		try {
			yield *this.node.join(bootstrapId)
			debug('[%s] successfully joined %s', this.id, bootstrapId)
			return
		}
		catch (err) {
			console.error('[' + this.id + '][join]', err)
			debug('[%s] join with %s failed, retrying(%d)', this.id, bootstrapId, retry)
			sleep(2000)
		}
	}
	this.started = false
	debug('[%s] failed to joined %s', this.id, bootstrapId)
	throw 'join failed'
}

Chord.prototype.boot = function *(opts) {
	if (opts.ioServer) {
		opts.ioServer.on('connection', sock => co(function *() {
			yield *this.acceptWebsocket(sock)
		}.bind(this)))
	}
	if (opts.ioClient) {
		yield waitOnce(opts.ioClient, 'connect')
		var id = yield *this.connectWebsocket(opts.ioClient)
		yield *this.join(id)
	}
	if (opts.localChord) {
		var id = yield *this.connectLocal(opts.localChord)
		yield *this.join(id)
	}
}

// api

Chord.prototype.start = function(opts) {
	if (!this.started) co(function *() {
		this.started = true

		if (opts)
			yield *this.boot(opts)

		while (this.started) {
			yield sleep(this.opts.stabilizeInterval)
			try {
				yield Promise.race([
					co(this.node.poll()),
					sleep(this.opts.nodePollTimeout),
				])
				yield *this.adjustNodeFingerSize()
			}
			catch (err) {
				console.error('[' + this.id + ']', err)
			}
		}

		this.started = false
	}.bind(this))

	return this
}

Chord.prototype.send = function(id, evt, data, ttl) {
	if (ttl === undefined) ttl = this.opts.forwardTTL
	this.buffers.push({ id, evt, data, ttl })

	if (!this.sending) co(function *() {
		this.sending = true

		while (buf = this.buffers.shift()) {
			co(this.forward(buf.id, buf.evt, buf.data, buf.ttl))
		}

		this.sending = false
	}.bind(this))
}

Chord.prototype.recv = function(callback) {
	if (callback)
		this.recvs.push(callback)
	return this
}

Chord.prototype.stop = function() {
	this.started = false
	this.hub.destroy()
}

Chord.prototype.sendVia = function(proxyId, id, evt, data) {
	var ttl = this.opts.forwardTTL
	this.send(proxyId, 'forward', { id, evt, data, ttl })
}

Chord.prototype.ping = function(id, evt, data) {
	this.sendVia(id, this.id, evt, data)
}

module.exports = Chord