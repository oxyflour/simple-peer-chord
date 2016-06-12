const debug = require('debug')('chord-hub'),
	co = require('co')

const sleep = (time, ret) => new Promise((resolve, reject) => setTimeout(resolve, time, ret))
const parseJson = function(text, fallback) { try { return JSON.parse(text) } catch(e) { return fallback } }

function Hub(opts, conn) {
	this.opts = Object.assign({

		minConnsToRecycle: 5,

		peerCallTimeout: 10000,

		peerConnectTimeout: 10000,

		peerRecycleTimeout: 30000,

	}, opts)

	this.conns = {
		// id -> peer
	}

	this.pending = {
		// id -> [{ resolve, reject, time, err }]
	}

	this.callbacks = {
		// token -> { resolve, reject, time, err }
	}

	this.conn = conn

	this.id = this.opts.id
}

Hub.prototype.recycle = function() {
	var now = Date.now(),
		ids = Object.keys(this.conns)
	if (ids.length > this.opts.minConnsToRecycle) ids.forEach(id => {
		if (!(now - this.conns[id].active < this.opts.peerRecycleTimeout)) {
			debug('recycling %s ... %s', this.id, id)
			this.remove(id)
		}
	})
}

Hub.prototype.get = function *(id) {
	this.recycle()

	if (!this.conns[id]) {
		debug('%s ... %s', this.id, id)

		if (!this.pending[id]) {
			this.pending[id] = [ ]
			co(this.conn.connect(id)).then(_ => {
				this.pending[id].forEach(cb => cb.resolve(this.conns[id]))
				delete this.pending[id]
			}).catch(err => {
				co(this.conn.onrecv('hub-conn-error', id))
				this.pending[id].forEach(cb => cb.reject(err))
				delete this.pending[id]
			})
		}

		var time = Date.now(),
			err = new Error('timeout')
		yield new Promise((resolve, reject) => {
			this.pending[id].push({ resolve, reject, time, err })
		})
	}

	this.conns[id].active = Date.now()
	return this.conns[id]
}

Hub.prototype.has = function(id) {
	return !!this.conns[id]
}

Hub.prototype.add = function(id, peer) {
	if (this.conns[id]) {
		if (this.conns[id] !== peer) {
			debug('%s already connected to %s, ignoring added peer', this.id, id)
			peer.destroy()
		}
		return this.conns[id]
	}

	peer.on('data', text => {
		var data = parseJson(text, { }), token = data.token
		if (data.evt) {
			co(this.conn.onrecv(data.evt, data.data)).then(ret => {
				token && peer.send(JSON.stringify({ token, ret }))
			}).catch(err => {
				if (err instanceof Error) err = err.message
				if (token) try {
					peer.send(JSON.stringify({ token, err }))
				}
				catch (e) {
					peer.destroy()
				}
			})
		}
		else if (data.token) {
			var cb = this.callbacks[token]
			if (cb) {
				if (typeof data.err === 'string') data.err = new Error(data.err)
				data.err ? cb.reject(data.err) : cb.resolve(data.ret)
				delete this.callbacks[token]
			}
		}
		peer.active = Date.now()
	})

	peer.on('error', err => {
		if (this.conns[id] === peer)
			this.remove(id)
		co(this.conn.onrecv('hub-peer-error', id))
	})

	peer.on('close', err => {
		if (this.conns[id] === peer)
			this.remove(id)
		co(this.conn.onrecv('hub-peer-close', id))
	})

	debug('%s <-> %s', this.id, id)
	peer.active = Date.now()
	return this.conns[id] = peer
}

Hub.prototype.remove = function(id) {
	var peer = this.conns[id]
	if (peer) {
		peer.destroy()
		debug('%s -x- %s', this.id, id)
		delete this.conns[id]
	}
}

Hub.prototype.call = function *(id, evt, data) {
	var peer = yield *this.get(id),
		callbacks = this.callbacks,
		time = Date.now(),
		err = new Error('call with ' + id + ' timeout'),
		token = id + '#' + Math.random() + '#' + time
	return yield new Promise((resolve, reject) => {
		peer.send(JSON.stringify({ token, evt, data }))
		callbacks[token] = { resolve, reject, time, err }
	})
}

Hub.prototype.send = function *(id, evt, data) {
	var peer = yield *this.get(id)
	peer.send(JSON.stringify({ evt, data }))
}

Hub.prototype.destroy = function() {
	Object.keys(this.pending).forEach(id => {
		this.pending[id].forEach(cb => cb.reject(new Error('destroyed')))
		this.pending[id] = [ ]
	})
	Object.keys(this.conns).forEach(id => {
		this.remove(id)
	})
}

Hub.prototype.checkCalls = function() {
	var now = Date.now()
	Object.keys(this.pending).forEach(id => {
		this.pending[id] = this.pending[id].filter(cb => {
			return cb.time > now - this.opts.peerConnectTimeout ||
				(cb.reject(cb.err), false)
		})
	})
	Object.keys(this.callbacks).forEach(token => {
		var cb = this.callbacks[token]
		if (!(cb.time > now - this.opts.peerCallTimeout)) {
			cb.reject(cb.err)
			delete this.callbacks[token]
		}
	})
}

module.exports = Hub