const NodeId = require('./node-id')()

// https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf
// https://arxiv.org/pdf/1502.06461.pdf

function Node(opts, conn) {
	this.opts = Object.assign({

		lookupTTL: NodeId.BITS,

		fingerTableSize: 32,

		succListSize: 32,

		fixFingerCocurrency: 8,

		succBackupCocurrency: 3,

		storeRecycleTimeout: 1 * 60 * 1000,

		subscriptionExpiration: 10 * 60 * 1000,

	}, opts)

	this.id = NodeId.create(this.opts.id)

	this.fingerIds = Array(this.opts.fingerTableSize).fill(this.id)

	this.succBackupIds = [ ] // does not include this.successorId

	Object.defineProperty(this, 'successorId', {
		get: _ => this.fingerIds[0],
		set: v => this.fingerIds[0] = v,
	})

	this.predBackupIds = [ ] // does not include this.successorId

	this.predecessorId = this.id

	this.storage = { }

	this.conn = conn
}

Node.prototype.findSuccessorId = function *(id) {
	return yield *this.recQuery(id, 'id')
}

Node.prototype.findPredecessorId = function *(id) {
	return yield *this.recQuery(id, 'predecessorId')
}

Node.prototype.put = function *(key, val) {
	key = NodeId.hash(key)
	return yield *this.recQuery(key, 'put', { key, val })
}

Node.prototype.get = function *(key) {
	key = NodeId.hash(key)
	return yield *this.recQuery(key, 'get', key)
}

Node.prototype.subscribe = function *(key, id) {
	key = NodeId.hash(key)
	id = id || this.id
	return yield *this.recQuery(key, 'subscribe', { key, id })
}

Node.prototype.unsubscribe = function *(key, id) {
	key = NodeId.hash(key)
	id = id || this.id
	return yield *this.recQuery(key, 'unsubscribe', { key, id })
}

Node.prototype.isResponsibleFor = function(id) {
	return NodeId.inRange(this.predecessorId, id, this.id) || id === this.id
}

Node.prototype._closestPrecedingFingerId = function(id) {
	for (var i = this.fingerIds.length - 1; i >= 0; i --) {
		if (NodeId.inRange(this.id, this.fingerIds[i], id)) {
			return this.fingerIds[i]
		}
	}
	return this.id
}

Node.prototype._successorList = function() {
	var ids = [this.successorId].concat(this.succBackupIds).slice(0, this.opts.succListSize)
	return ids.filter((id, i) => id !== this.id && ids.indexOf(id) === i)
}

Node.prototype._predecessorList = function() {
	var ids = [this.predecessorId].concat(this.predBackupIds).slice(0, this.opts.succListSize)
	return ids.filter((id, i) => id !== this.id && ids.indexOf(id) === i)
}

Node.prototype.stablize = function *() {
	var preId = yield *this.conn.query(this.successorId, 'predecessorId')
	if (NodeId.inRange(this.id, preId, this.successorId))
		this.successorId = preId

	this.succBackupIds = yield *this.conn.query(this.successorId, 'succWithBackupIds')
	this.predBackupIds = yield *this.conn.query(this.predecessorId, 'predWithBackupIds')

	yield *this.conn.notify(this.successorId, 'notify', this.id)
}

Node.prototype._notify = function(id) {
	if (this.predecessorId === this.id ||
		NodeId.inRange(this.predecessorId, id, this.id)) {
		this.predecessorId = id
	}
}

Node.prototype.fixFinger = function *(i) {
	var id = NodeId.fingerStart(this.id, i)
	this.fingerIds[i] = yield *this.findSuccessorId(id)
	// keep a live connection to the finger
	yield *this.conn.notify(this.fingerIds[i], 'id')
}

Node.prototype._get = function(key) {
	var data = this.storage[key]
	return data && (data.active = Date.now()) && data.val
}

Node.prototype._hash = function(key) {
	var data = this.storage[key]
	return data && (data.active = Date.now()) && data.hash
}

Node.prototype._put = function(key, val) {
	var active = Date.now(),
		hash = NodeId.hash(val)
	this.storage[key] = { val, active, hash }
}

Node.prototype._keys = function() {
	return Object.keys(this.storage)
}

Node.prototype._recycleStore = function() {
	var timeout = Date.now() - this.opts.storeRecycleTimeout
	this._keys().forEach(key => {
		if (!(this.storage[key].active > timeout)) {
			delete this.storage[key]
		}
	})
}

Node.prototype._subscribe = function(key, id) {
	var saved = this._get(key) || { },
		expiration = Date.now() - this.opts.subscriptionExpiration,
		dict = { }
	Object.keys(saved)
		.filter(id => saved[id] > expiration)
		.forEach(id => dict[id] = saved[id])
	dict[id] = Date.now()
	this._put(key, dict)
	return dict
}

Node.prototype._unsubscribe = function(key, id) {
	var dict = this._get(key) || { }
	delete dict[id]
	this._put(key, dict)
	return dict
}

Node.prototype.replicate = function *(successorId, key) {
	var hash = yield *this.conn.query(successorId, 'hash', key)
	if (hash !== this._hash(key)) {
		var val = this._get(key)
		yield *this.conn.notify(successorId, 'put', { key, val })
	}
}

Node.prototype.join = function *(bootstrapId) {
	var successorId = yield *this.conn.query(bootstrapId, 'findSuccessorId', this.id),
		succBackupIds = yield *this.conn.query(successorId, 'succWithBackupIds'),
		predecessorId = yield *this.conn.query(bootstrapId, 'findPredecessorId', this.id),
		predBackupIds = yield *this.conn.query(predecessorId, 'predWithBackupIds')

	// initialize fingers
	var startIds = this.fingerIds.map((id, i) => NodeId.fingerStart(this.id, i)),
		fingerIds = yield startIds.map(id => this.conn.query(bootstrapId, 'findSuccessorId', id))
	yield fingerIds.map(id => this.conn.notify(id, 'id'))

	// connect to the successor to sync storage
	var keys = yield *this.conn.query(successorId, 'keys'),
		storedKeys = keys.filter(key => this.isResponsibleFor(key)),
		storedVals = yield storedKeys.map(key => this.conn.query(successorId, 'get', key))

	// connect to more successors
	yield succBackupIds.slice(0, this.opts.succBackupCocurrency).map(id => this.conn.notify(id, 'id'))

	// ok to join
	Object.assign(this, { succBackupIds, predecessorId, predBackupIds, fingerIds })
	storedKeys.forEach((key, i) => this._put(key, storedVals[i]))
}

Node.prototype.remove = function(id) {
	var newSuccId = this.id
	if (this.predecessorId === id) {
		newSuccId = this.predecessorId = this.predBackupIds.shift() || this.id
	}
	else if (this.successorId === id) {
		newSuccId = this.successorId = this.succBackupIds.shift() || this.id
	}
	else if (this.succBackupIds.indexOf(id) >= 0) {
		newSuccId = this.succBackupIds[this.succBackupIds.indexOf(id) + 1] || this.id
	}
	this.succBackupIds = this.succBackupIds.filter(i => i !== id)
	this.predBackupIds = this.predBackupIds.filter(i => i !== id)
	this.fingerIds = this.fingerIds.map(i => i !== id ? i : newSuccId)
}

Node.prototype.recQuery = function *(id, method, arg, ttl) {
	ttl = ttl !== undefined ? ttl : this.opts.lookupTTL
	var nextId
	if (!(ttl -- > 0)) {
		throw 'ttl'
	}
	else if (this.isResponsibleFor(id)) {
		return yield *this.query(method, arg)
	}
	else if ((nextId = this._closestPrecedingFingerId(id)) === this.id) {
		return yield *this.conn.query(this.successorId, method, arg)
	}
	else {
		return yield *this.conn.query(nextId, 'recQuery', { id, method, arg, ttl })
	}
}

Node.prototype.query = function *(method, arg) {
	if (method === 'id') {
		return this.id
	}
	else if (method === 'successorId') {
		return this.successorId
	}
	else if (method === 'predecessorId') {
		return this.predecessorId
	}
	else if (method === 'succWithBackupIds') {
		return this._successorList()
	}
	else if (method === 'predWithBackupIds') {
		return this._predecessorList()
	}
	else if (method === 'notify') {
		return this._notify(arg)
	}

	else if (method === 'keys') {
		return this._keys()
	}
	else if (method === 'get') {
		return this._get(arg)
	}
	else if (method === 'hash') {
		return this._hash(arg)
	}
	else if (method === 'put') {
		return this._put(arg.key, arg.val)
	}
	else if (method === 'subscribe') {
		return this._subscribe(arg.key, arg.id)
	}
	else if (method === 'unsubscribe') {
		return this._unsubscribe(arg.key, arg.id)
	}

	else if (method === 'recQuery') {
		return yield *this.recQuery(arg.id, arg.method, arg.arg, arg.ttl)
	}
	else if (method === 'findSuccessorId') {
		return yield *this.findSuccessorId(arg)
	}
	else if (method === 'findPredecessorId') {
		return yield *this.findPredecessorId(arg)
	}

	else {
		throw 'unknown method!'
	}
}

Node.prototype.poll = function() {
	var fingerSize = this.fingerIds.length - 1,
		maxConns = Math.min(this.opts.fixFingerCocurrency, fingerSize),
		randomIndex = Math.floor(Math.random() * fingerSize),
		fingerIndices = Array(maxConns).fill(randomIndex).map((i, j) => (i + j) % fingerSize),
		fingerQueries = fingerIndices.map(i => this.fixFinger(i + 1)),

		successorsToSync = this._successorList().slice(0, this.opts.succBackupCocurrency + 1),
		localStorageKeys = this._keys().filter(id => this.isResponsibleFor(id)),
		idToSync = localStorageKeys[Math.floor(Math.random() * localStorageKeys.length)],
		syncQueries = successorsToSync.map(id => this.replicate(id, idToSync)),

		stablizationQuery = this.stablize()

	this._recycleStore()

	return fingerQueries.concat(syncQueries).concat(stablizationQuery)
}

module.exports = Node
