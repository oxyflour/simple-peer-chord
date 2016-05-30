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

function Chord(opts, bootstrap) {
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

	this.debug = function(message) {
		var msg = '[%s] ' + message,
			args = [].slice.call(arguments, 1)
		debug.apply(null, [msg, this.id].concat(args))
	}

	this.recvs = [ ]
	this.buffers = [ ]

	this.started = false
	this.sending = false

	this.id = this.node.id

	bootstrap && setTimeout(_ => this.start(bootstrap))
}

// interface for node

Chord.prototype.pull = function *(id, method, arg) {
	try {
		return yield *this.hub.call(id, 'node-pull', { method, arg })
	}
	catch (err) {
		this.debug('node %s seems dead', id)
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
		co(this.connectViaProxy(data.id, data.proxyId, data.token))
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
		yield *this.connectViaProxy(id, proxyId, token, true)

		conns[id].uptime = Date.now()
		return conns[id]
	}
	else {
		this.started = false
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
		this.debug('forwarding "%s" to %s, ttl %s', evt, id, ttl)
		yield *this.hub.send(id, evt, data)
	}
	else if ((nextId = this.node._closestPrecedingFingerId(id)) === this.id) {
		throw 'no route to ' + id
	}
	else {
		this.debug('forwarding "%s" to %s via %s, ttl %s', evt, id, nextId, ttl)
		yield *this.hub.send(nextId, 'forward', { id, evt, data, ttl })
	}
}

// connections

Chord.prototype.connectViaProxy = function *(id, proxyId, token, initiator) {
	this.sendVia(proxyId, id, 'signal-ready', { token })
	yield Promise.race([
		timeout(this.opts.signalTimeout,
			'wait timeout when connecting to ' + id + ' via ' + proxyId),
		new Promise(resolve => this.recv((evt, data) =>
			evt === 'signal-ready' && data.token === token && (resolve() || true)))
	])

	this.debug('connecting to %s via node %s...', id, proxyId)

	var peer = new SimplePeer(Object.assign({ initiator }, this.opts.peerOptions))

	peer.on('signal', offer => {
		this.sendVia(proxyId, id, 'signal-offer', { offer, token })
	})

	this.recv((evt, data) => {
		if (evt === 'signal-offer' && data.token === token)
			peer.signal(data.offer)
		return this.hub.has(id)
	})

	yield waitOnce(peer, 'connect', this.opts.signalTimeout)
	this.hub.add(id, peer)
}

Chord.prototype.connectWebsocket = function *(sock, initiator, timeout) {
	sock.emit('id', this.id)
	var id = yield waitOnce(sock, 'id', timeout || this.opts.signalTimeout)

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
			this.debug('reducing finger size to %d', fingerIds.length)
		}
		else if (connCount < this.opts.minHubConnCount &&
				fingerIds.length < Math.min(this.opts.maxNodeFingerSize, this.node.opts.fingerTableSize)) {
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
			console.error('[' + this.id + '][join]', err)
			this.debug('join with %s failed, retrying(%d)', bootstrapId, retry)
			sleep(2000)
		}
	}
	this.started = false
	this.debug('failed to joined %s', bootstrapId)
	throw 'join failed'
}

// api

Chord.prototype.start = function(bootstrap) {
	return co(function *() {
		if (bootstrap instanceof Chord) {
			var id = yield *this.connectLocal(bootstrap)
			yield *this.joinOrRetry(id)
		}
		else if (bootstrap && bootstrap.url) {
			var sock = io(bootstrap.url, bootstrap.opts)
			yield waitOnce(sock, 'connect')
			sock.emit('join', bootstrap.channel)
			var initiator = yield waitOnce(sock, 'joined'),
				id = yield *this.connectWebsocket(sock, initiator)
			if (initiator)
				yield *this.joinOrRetry(id)
		}

		yield *this.onrecv('chord-start')

		if (!this.started) {
			this.started = true

			while (this.started) {
				yield sleep(this.opts.stabilizeInterval)
				try {
					yield Promise.race([
						Promise.all(this.node.poll().map(co)),
						sleep(this.opts.nodePollTimeout),
					])
					yield *this.adjustNodeFingerSize()
				}
				catch (err) {
					console.error('[' + this.id + ']', err)
				}
			}

			this.started = false
		}
	}.bind(this))
}

Chord.prototype.send = function(id, evt, data, ttl) {
	if (ttl === undefined) ttl = this.opts.forwardTTL
	this.buffers.push({ id, evt, data, ttl })

	return co(function *() {
		if (!this.sending) {
			this.sending = true

			while (buf = this.buffers.shift()) {
				co(this.forward(buf.id, buf.evt, buf.data, buf.ttl))
			}

			this.sending = false
		}
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
	return this.send(proxyId, 'forward', { id, evt, data, ttl })
}

Chord.prototype.ping = function(id, evt, data) {
	return this.sendVia(id, this.id, evt, data)
}

module.exports = Chord