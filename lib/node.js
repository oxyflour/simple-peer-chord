const NodeId = require('./node-id')()

// https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf
// https://arxiv.org/pdf/1502.06461.pdf

function Node(opts, conn) {
	this.opts = Object.assign({

		lookupTTL: NodeId.BITS,

		fingerTableSize: NodeId.BITS,

		succListSize: NodeId.BITS,

		fixFingerCocurrency: 5,

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
	return yield *this.lookup(id, 'id', undefined, this.opts.lookupTTL)
}

Node.prototype.findPredecessorId = function *(id) {
	return yield *this.lookup(id, 'predecessorId', undefined, this.opts.lookupTTL)
}

Node.prototype.put = function *(key, val) {
	key = NodeId.hash(key)
	return yield *this.lookup(key, 'put', { key, val }, this.opts.lookupTTL)
}

Node.prototype.get = function *(key) {
	key = NodeId.hash(key)
	return yield *this.lookup(key, 'get', key, this.opts.lookupTTL)
}

Node.prototype.subscribe = function *(key, id) {
	key = NodeId.hash(key)
	id = id || this.id
	return yield *this.lookup(key, 'subscribe', { key, id }, this.opts.lookupTTL)
}

Node.prototype.unsubscribe = function *(key, id) {
	key = NodeId.hash(key)
	id = id || this.id
	return yield *this.lookup(key, 'unsubscribe', { key, id }, this.opts.lookupTTL)
}

Node.prototype.isResponsibleFor = function(id) {
	return NodeId.inRange(this.predecessorId, id, this.id) || id === this.id
}

Node.prototype._closestPrecedingFingerId = function(id) {
	for (var i = this.fingerIds.length - 1; i >= 0; i --)
		if (NodeId.inRange(this.id, this.fingerIds[i], id))
			return this.fingerIds[i]
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
	var preId = yield *this.pull(this.successorId, 'predecessorId')
	if (NodeId.inRange(this.id, preId, this.successorId))
		this.successorId = preId

	this.succBackupIds = yield *this.pull(this.successorId, 'succWithBackupIds')
	this.predBackupIds = yield *this.pull(this.predecessorId, 'predWithBackupIds')

	yield *this.pull(this.successorId, 'notify', this.id)
}

Node.prototype._notify = function(id) {
	if (this.predecessorId === this.id ||
		NodeId.inRange(this.predecessorId, id, this.id))
		this.predecessorId = id
}

Node.prototype.fixFinger = function *(i) {
	var id = NodeId.fingerStart(this.id, i)
	this.fingerIds[i] = yield *this.findSuccessorId(id)
	// keep a live connection to the finger
	yield *this.pull(this.fingerIds[i], 'id')
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
		if (!(this.storage[key].active > timeout))
			delete this.storage[key]
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
	var hash = yield *this.pull(successorId, 'hash', key)
	if (hash !== this._hash(key)) {
		var val = this._get(key)
		yield *this.pull(successorId, 'put', { key, val })
	}
}

Node.prototype.join = function *(bootstrapId) {
	this.successorId = yield *this.pull(bootstrapId, 'findSuccessorId', this.id)
	this.succBackupIds = yield *this.pull(this.successorId, 'succWithBackupIds')

	this.predecessorId = yield *this.pull(bootstrapId, 'findPredecessorId', this.id)
	this.predBackupIds = yield *this.pull(this.predecessorId, 'predWithBackupIds')

	// connect to the successor to sync the storage
	var storedKeys = yield *this.pull(this.successorId, 'keys')
	for (var i = 0; i < storedKeys.length; i ++) {
		var key = storedKeys[i]
		if (this.isResponsibleFor(key))
			this._put(key, yield *this.pull(this.successorId, 'get', key))
	}

	// initialize fingers except the first one (the successor)
	for (var i = 1; i < this.fingerIds.length; i ++) {
		var id = NodeId.fingerStart(this.id, i)
		this.fingerIds[i] = yield *this.pull(bootstrapId, 'findSuccessorId', id)
		yield *this.pull(this.fingerIds[i], 'successorId')
	}

	// connect to more successors in case of failures
	for (var i = 0; i < this.succBackupIds.length && i < this.opts.succBackupCocurrency; i ++) {
		yield *this.pull(this.succBackupIds[i], 'id')
	}
}

Node.prototype.remove = function(id) {
	var newSuccId = this.id
	if (this.predecessorId === id)
		newSuccId = this.predecessorId = this.predBackupIds.shift() || this.id
	else if (this.successorId === id)
		newSuccId = this.successorId = this.succBackupIds.shift() || this.id
	else if (this.succBackupIds.indexOf(id) >= 0)
		newSuccId = this.succBackupIds[this.succBackupIds.indexOf(id) + 1] || this.id
	this.succBackupIds = this.succBackupIds.filter(i => i !== id)
	this.predBackupIds = this.predBackupIds.filter(i => i !== id)
	this.fingerIds = this.fingerIds.map(i => i !== id ? i : newSuccId)
}

Node.prototype.lookup = function *(id, method, arg, ttl) {
	var nextId
	if (!(ttl -- > 0))
		throw 'ttl'
	else if (this.isResponsibleFor(id))
		return yield *this.pull(this.id, method, arg)
	else if ((nextId = this._closestPrecedingFingerId(id)) === this.id)
		return yield *this.pull(this.successorId, method, arg)
	else
		return yield *this.pull(nextId, 'lookup', { id, method, arg, ttl })
}

Node.prototype.pull = function *(id, method, arg) {
	if (id === this.id) {
		if (method === 'id')
			return this.id
		else if (method === 'successorId')
			return this.successorId
		else if (method === 'predecessorId')
			return this.predecessorId
		else if (method === 'succWithBackupIds')
			return this._successorList()
		else if (method === 'predWithBackupIds')
			return this._predecessorList()
		else if (method === 'notify')
			return this._notify(arg)

		else if (method === 'lookup')
			return yield *this.lookup(arg.id, arg.method, arg.arg, arg.ttl)
		else if (method === 'findSuccessorId')
			return yield *this.findSuccessorId(arg)
		else if (method === 'findPredecessorId')
			return yield *this.findPredecessorId(arg)

		else if (method === 'keys')
			return this._keys()
		else if (method === 'get')
			return this._get(arg)
		else if (method === 'hash')
			return this._hash(arg)
		else if (method === 'put')
			return this._put(arg.key, arg.val)
		else if (method === 'subscribe')
			return this._subscribe(arg.key, arg.id)
		else if (method === 'unsubscribe')
			return this._unsubscribe(arg.key, arg.id)
		else
			throw 'unknown method!'
	}
	else {
		return yield *this.conn.pull(id, method, arg)
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
