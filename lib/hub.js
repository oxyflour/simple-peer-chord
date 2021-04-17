const debug = require('debug')('chord-hub'),
	co = require('co')

const sleep = (time, ret) => new Promise((resolve, reject) => setTimeout(resolve, time, ret))
const parseJson = function(text, fallback) { try { return JSON.parse(text) } catch(e) { return fallback } }

class Hub {
	constructor (opts, conn) {
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

		this.failedConns = {
			// id -> time
		}

		this.conn = conn

		this.id = this.opts.id
	}

	recycle () {
		var now = Date.now()
		var ids = Object.keys(this.conns)

		if (ids.length > this.opts.minConnsToRecycle)
			ids.forEach(id => {
				if (!(now - this.conns[id].active < this.opts.peerRecycleTimeout)) {
					debug('recycling %s ... %s', this.id, id)
					this.remove(id)
				}
			})
	}

	* get (id) {
		this.recycle()

		if (this.failedConns[id] > Date.now() - 1000) {
			throw 'connection to ' + id + ' has just failed. retry later'
		}

		if (!this.conns[id]) {
			debug('%s ... %s', this.id, id)

			if (!this.pendingConns[id]) {
				this.pendingConns[id] = []
				co(this.conn.connect(id, this.conns)).then(_ => {
					this.pendingConns[id].forEach(cb => cb.resolve(this.conns[id]))
					delete this.pendingConns[id]
				}).catch(err => {
					this.pendingConns[id].forEach(cb => cb.reject(err))
					delete this.pendingConns[id]
					this.conn.emit('-hub-conn-error', id)
					this.failedConns[id] = Date.now()
				})
			}

			var time = Date.now()
			var err = new Error('timeout')
			yield new Promise((resolve, reject) => {
				this.pendingConns[id].push({ resolve, reject, time, err })
			})
		}

		this.conns[id].active = Date.now()
		return this.conns[id]
	}

	has (id) {
		return !!this.conns[id]
	}

	add (id, peer) {
		if (this.conns[id]) {
			if (this.conns[id] !== peer) {
				debug('%s already connected to %s, ignoring added peer', this.id, id)
				peer.destroy()
			}
			return this.conns[id]
		}

		peer.bytesSent = peer.byteRecv = 0

		peer.sendSafe = function (str) {
			peer.bytesSent += str.length
			try {
				peer.send(str)
			}
			catch (err) {
				peer.destroy()
				throw err
			}
		}

		peer.on('data', text => {
			var data = parseJson(text, {})
			if (data.req)
				co(function* () {
					var res = data.req
					try {
						var ret = yield this.conn.query(this.conn.id, data.method, data.arg)
						peer.sendSafe(JSON.stringify({ res, ret }))
					}
					catch (err) {
						if (err instanceof Error) {
							err = err.message
						}
						peer.sendSafe(JSON.stringify({ res, err }))
					}
				}.bind(this))
			else if (data.res) {
				var cb = this.callbacks[data.res]
				if (cb) {
					if (typeof data.err === 'string') {
						data.err = new Error(data.err)
					}
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

	remove (id) {
		var peer = this.conns[id]
		if (peer) {
			delete this.conns[id]
			peer.destroy()
			debug('%s -x- %s (sent: %d, recv: %d)', this.id, id, peer.bytesSent, peer.byteRecv)
		}
	}

	* query (id, method, arg) {
		var peer = yield* this.get(id)
		var time = Date.now()
		var err = new Error(this.id + ' query with ' + id + ' (' + method + ') timeout')
		var req = id + '#' + Math.random() + '#' + time

		peer.sendSafe(JSON.stringify({ req, method, arg }))
		return yield new Promise((resolve, reject) => {
			this.callbacks[req] = { id, resolve, reject, time, err }
		})
	}

	* send (id, evt, data) {
		var peer = yield* this.get(id)
		peer.sendSafe(JSON.stringify({ evt, data }))
	}

	destroy () {
		Object.keys(this.pendingConns).forEach(id => {
			this.pendingConns[id].forEach(cb => cb.reject(new Error('destroyed')))
			this.pendingConns[id] = []
		})
		Object.keys(this.conns).forEach(id => {
			this.remove(id)
		})
	}

	checkTimeout () {
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
}

module.exports = Hub
