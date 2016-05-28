const debug = require('debug')('chord-node'),
	co = require('co')

const NodeId = require('./node-id')

// https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf
// https://arxiv.org/pdf/1502.06461.pdf

function Node(opts, conn) {
	this.opts = Object.assign({

		lookupTTL: 128,

		fingerTableSize: NodeId.BITS,

		succListSize: NodeId.BITS,

		fixFingerCocurrency: 5,

		succBackupCocurrency: 2,

		pollTimeout: 3000,

	}, opts)

	this.id = NodeId.from(this.opts.id)

	this.fingerIds = Array(this.opts.fingerTableSize).fill(this.id)

	this.succBackupIds = [ ] // does not include this.successorId

	this.predecessorId = undefined

	Object.defineProperty(this, 'successorId', {
		get: _ => this.fingerIds[0],
		set: v => this.fingerIds[0] = v,
	})

	this.conn = conn
}

Node.prototype.findSuccessorId = function *(id) {
	return yield *this.lookup(id, 'successorId', undefined, this.opts.lookupTTL)
}

Node.prototype.findPredecessorId = function *(id) {
	return yield *this.lookup(id, 'id', undefined, this.opts.lookupTTL)
}

Node.prototype._closestPrecedingFingerId = function(id) {
	for (var i = this.fingerIds.length - 1; i >= 0; i --)
		if (NodeId.inRange(this.id, this.fingerIds[i], id))
			return this.fingerIds[i]
	return this.id
}

Node.prototype._succWithBackupIds = function() {
	var ids = [this.successorId].concat(this.succBackupIds)
		.slice(0, this.opts.succListSize)
	return ids.filter((id, i) => id !== this.id && ids.indexOf(id) === i)
}

Node.prototype.stablize = function *() {
	var preId = yield *this.pull(this.successorId, 'predecessorId')
	if (NodeId.inRange(this.id, preId, this.successorId))
		this.successorId = preId
	this.succBackupIds = yield *this.pull(this.successorId, 'succWithBackupIds')
	yield *this.pull(this.successorId, 'notify', this.id)
}

Node.prototype._notify = function(id) {
	if (this.predecessorId === undefined ||
		NodeId.inRange(this.predecessorId, id, this.id))
		this.predecessorId = id
}

Node.prototype.fixFinger = function *(i) {
	this.fingerIds[i] = yield *this.findSuccessorId(NodeId.shift(this.id, i))

	// keep a live connection to the finger
	yield *this.pull(this.fingerIds[i], 'successorId')
}

Node.prototype.join = function *(bootstrapId) {
	this.successorId = yield *this.pull(bootstrapId, 'findSuccessorId', this.id)
	this.succBackupIds = yield *this.pull(this.successorId, 'succWithBackupIds')

	// Note: The following steps are not necessary from the original papers,
	//       but doing these before we start affecting the network makes it robuster

	// connect the predecessor now or will be connected later
	this.predecessorId = yield *this.pull(bootstrapId, 'findPredecessorId', this.id)
	if (this.predecessorId) yield *this.pull(this.predecessorId, 'successorId')

	// initialize fingers except the first one (the successor)
	for (var i = 1; i < this.fingerIds.length; i ++) {
		this.fingerIds[i] = yield *this.pull(bootstrapId, 'findSuccessorId', NodeId.shift(this.id, i))
		yield *this.pull(this.fingerIds[i], 'successorId')
	}

	// connect to more successors in case of failures
	for (var i = 0; i < this.succBackupIds.length && i < this.opts.succBackupCocurrency; i ++) {
		yield *this.pull(this.succBackupIds[i], 'successorId')
	}
}

Node.prototype.remove = function(id) {
	debug('[%s] removing %s', this.id, id)
	if (this.predecessorId === id) {
		this.predecessorId = undefined
	}
	else {
		var newSuccId
		if (this.successorId === id)
			newSuccId = this.successorId = this.succBackupIds.shift() || this.id
		else if (this.succBackupIds.indexOf(id) >= 0)
			newSuccId = this.succBackupIds[this.succBackupIds.indexOf(id) + 1] || this.id
		this.succBackupIds = this.succBackupIds.filter(i => i !== id)
		this.fingerIds = this.fingerIds.map(i => i !== id ? i : newSuccId)
	}
}

Node.prototype.lookup = function *(id, method, arg, ttl) {
	if (NodeId.inRange(this.id, id, this.successorId) || id === this.successorId) {
		return yield *this.pull(this.id, method, arg)
	}
	else if (ttl -- > 0) {
		var nextId = this._closestPrecedingFingerId(id)
		return nextId === this.id ?
			yield *this.pull(this.id, method, arg) :
			yield *this.pull(nextId, 'lookup', { id, method, arg, ttl })
	}
	else {
		throw 'ttl'
	}
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
			return this._succWithBackupIds()
		else if (method === 'notify')
			return this._notify(arg)
		else if (method === 'lookup')
			return yield *this.lookup(arg.id, arg.method, arg.arg, arg.ttl)
		else if (method === 'findSuccessorId')
			return yield *this.findSuccessorId(arg)
		else if (method === 'findPredecessorId')
			return yield *this.findPredecessorId(arg)
		else
			throw 'unknown method!'
	}
	else {
		return yield *this.conn.pull(id, method, arg)
	}
}

Node.prototype.poll = function *() {
	var fingerSize = this.fingerIds.length - 1,
		maxConns = Math.min(this.opts.fixFingerCocurrency, fingerSize),
		randomIndex = Math.floor(Math.random() * fingerSize),
		fingerIndices = Array(maxConns).fill(randomIndex).map((i, j) => (i + j) % fingerSize),
		fingerQueries = fingerIndices.map(i => co(this.fixFinger(i + 1))),

		successorIndices = this.succBackupIds.slice(0, this.opts.succBackupCocurrency),
		succQueries = successorIndices.map(id => co(this.pull(id, 'successorId'))),

		stablizationQuery = co(this.stablize())

	yield Promise.race([
		new Promise(next => setTimeout(next, this.opts.pollTimeout)),
		Promise.all(fingerQueries.concat(succQueries).concat(stablizationQuery)),
	])
}

module.exports = Node
