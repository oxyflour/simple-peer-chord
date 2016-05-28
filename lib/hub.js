const debug = require('debug')('chord-hub'),
	co = require('co')

const sleep = (time, ret) => new Promise((resolve, reject) => setTimeout(resolve, time, ret))
const parseJson = function(text, fallback) { try { return JSON.parse(text) } catch(e) { return fallback } }

function *lock(obj, key, timeout) {
	key = 'chord-lock-' + key

	var up = Date.now()
	while (obj[key] && !(Date.now() - up > timeout)) {
		yield sleep(200)
	}

	obj[key] = true
	return function() {
		obj[key] = false
	}
}

function Hub(opts, conn) {
	this.opts = Object.assign({

		minConnsToRecycle: 5,

		peerCallTimeout: 30000,

		peerRecycleTimeout: 30000,

		peerLockTimeout: 3000,

	}, opts)

	this.conns = {
		// id -> peer
	}

	this.callbacks = {
		// token -> { resolve, reject, time, err }
	}

	this.conn = conn

	this.id = this.opts.id
}

Hub.prototype.recycle = function *() {
	var now = Date.now(),
		ids = Object.keys(this.conns)
	if (ids.length > this.opts.minConnsToRecycle) ids.forEach(id => {
		if (!(now - this.conns[id].active < this.opts.peerRecycleTimeout)) {
			debug('[%s] recycling %s...', this.id, id)
			this.remove(id)
		}
	})
}

Hub.prototype.get = function *(id) {
	yield *this.recycle()

	if (!this.conns[id]) {
		debug('%s ... %s', this.id, id)

		var unlock = yield *lock(this, 'connect', this.opts.peerLockTimeout)

		if (!this.conns[id]) try {
			var peer = yield *this.conn.connect(id)
			this.add(id, peer)
		}
		catch (err) {
			unlock()
			throw err
		}

		unlock()
	}

	this.conns[id].active = Date.now()
	return this.conns[id]
}

Hub.prototype.has = function(id) {
	return !!this.conns[id]
}

Hub.prototype.add = function(id, peer) {
	if (this.conns[id]) {
		debug('[%s] node %s already connected, ignoring added peer', this.id, id)
		if (this.conns[id] !== peer)
			peer.destroy()
		return this.conns[id]
	}

	peer.on('data', text => {
		var data = parseJson(text, { }), token = data.token
		if (data.evt) {
			co(this.conn.onrecv(data.evt, data.data)).then(ret => {
				token && peer.send(JSON.stringify({ token, ret }))
			}).catch(err => {
				token && peer.send(JSON.stringify({ err }))
			})
		}
		else if (data.token) {
			var cb = this.callbacks[token]
			if (cb) {
				data.err ? cb.reject(data.err) : cb.resolve(data.ret)
				delete this.callbacks[token]
			}
		}
		peer.active = Date.now()
	})

	peer.on('error', err => {
		console.error('[simple-peer]', err)
		this.remove(id)
		co(this.conn.onrecv('peer-error', id))
	})

	peer.on('close', err => {
		this.remove(id)
		co(this.conn.onrecv('peer-close', id))
	})

	debug('%s <-> %s', this.id, id)
	peer.active = Date.now()
	return this.conns[id] = peer
}

Hub.prototype.remove = function(id) {
	debug('%s -x- %s', this.id, id)
	if (this.conns[id]) {
		this.conns[id].destroy()
		delete this.conns[id]
	}
}

Hub.prototype.call = function *(id, evt, data) {
	var peer = yield *this.get(id),
		callbacks = this.callbacks,
		time = Date.now(),
		token = id + '#' + Math.random() + '#' + time
	return yield Promise.race([
		new Promise((resolve, reject) => {
			peer.send(JSON.stringify({ token, evt, data }))
			callbacks[token] = { resolve, reject }
		}),
		new Promise((resolve, reject) => setTimeout(_ => {
			delete callbacks[token]
			reject()
		}, this.opts.peerCallTimeout))
	])
}

Hub.prototype.send = function *(id, evt, data) {
	var peer = yield *this.get(id)
	peer.send(JSON.stringify({ evt, data }))
}

Hub.prototype.destroy = function() {
	Object.keys(this.conns).forEach(id => this.remove(id))
}

module.exports = Hub