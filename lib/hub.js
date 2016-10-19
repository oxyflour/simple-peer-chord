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

	this.pendingConns = {
		// id -> [{ resolve, reject, time, err }]
	}

	this.callbacks = {
		// token -> { id, resolve, reject, time, err }
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

		if (!this.pendingConns[id]) {
			this.pendingConns[id] = [ ]
			co(this.conn.connect(id, this.conns)).then(_ => {
				this.pendingConns[id].forEach(cb => cb.resolve(this.conns[id]))
				delete this.pendingConns[id]
			}).catch(err => {
				this.pendingConns[id].forEach(cb => cb.reject(err))
				delete this.pendingConns[id]
				this.conn.emit('-hub-conn-error', id)
			})
		}

		var time = Date.now(),
			err = new Error('timeout')
		yield new Promise((resolve, reject) => {
			this.pendingConns[id].push({ resolve, reject, time, err })
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

	peer.bytesSent = peer.byteRecv = 0

	peer.sendStr = function(str) {
		peer.bytesSent += str.length
		peer.send(str)
	}

	peer.on('data', text => {
		var data = parseJson(text, { })
		if (data.req) co(function *() {
			var res = data.req
			try {
				var ret = yield this.conn.call(this.conn.id, data.method, data.arg)
				peer.sendStr(JSON.stringify({ res, ret }))
			}
			catch (err) {
				if (err instanceof Error) err = err.message
				try {
					peer.sendStr(JSON.stringify({ res, err }))
				}
				catch (err) {
					peer.destroy()
				}
			}
		}.bind(this))
		else if (data.res) {
			var cb = this.callbacks[data.res]
			if (cb) {
				if (typeof data.err === 'string') data.err = new Error(data.err)
				data.err ? cb.reject(data.err) : cb.resolve(data.ret)
				delete this.callbacks[data.res]
			}
		}
		else if (data.evt) {
			this.conn.emit(data.evt, data.data)
		}
		peer.byteRecv += text.length
		peer.active = Date.now()
	})

	peer.on('error', err => {
		if (this.conns[id] === peer) {
			this.remove(id)
		}
		this.conn.emit('-hub-peer-error', id)
	})

	peer.on('close', err => {
		if (this.conns[id] === peer) {
			this.remove(id)
		}
		this.conn.emit('-hub-peer-close', id)
	})

	debug('%s <-> %s', this.id, id)
	peer.active = Date.now()
	return this.conns[id] = peer
}

Hub.prototype.remove = function(id) {
	var peer = this.conns[id]
	if (peer) {
		delete this.conns[id]
		peer.destroy()
		debug('%s -x- %s (sent: %d, recv: %d)', this.id, id, peer.bytesSent, peer.byteRecv)
	}
}

Hub.prototype.call = function *(id, method, arg) {
	var peer = yield *this.get(id),
		time = Date.now(),
		err = new Error(this.id + ' call with ' + id + ' (' + method + ') timeout'),
		req = id + '#' + Math.random() + '#' + time
	peer.sendStr(JSON.stringify({ req, method, arg }))
	return yield new Promise((resolve, reject) => {
		this.callbacks[req] = { id, resolve, reject, time, err }
	})
}

Hub.prototype.send = function *(id, evt, data) {
	var peer = yield *this.get(id)
	peer.sendStr(JSON.stringify({ evt, data }))
}

Hub.prototype.destroy = function() {
	Object.keys(this.pendingConns).forEach(id => {
		this.pendingConns[id].forEach(cb => cb.reject(new Error('destroyed')))
		this.pendingConns[id] = [ ]
	})
	Object.keys(this.conns).forEach(id => {
		this.remove(id)
	})
}

Hub.prototype.checkCalls = function() {
	var now = Date.now()
	Object.keys(this.pendingConns).forEach(id => {
		this.pendingConns[id] = this.pendingConns[id].filter(cb => {
			if (!(cb.time > now - this.opts.peerConnectTimeout)) {
				cb.reject(cb.err)
				return false
			}
			return true
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