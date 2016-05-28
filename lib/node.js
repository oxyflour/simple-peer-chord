const debug = require('debug')('chord-node'),
	co = require('co')

const NodeId = require('./node-id')

// https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf
// https://arxiv.org/pdf/1502.06461.pdf

function Node(opts, conn) {
	this.opts = Object.assign({

		queryTTL: 128,

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

/*
 * interative looking up. we don't use it as it needs more connections
 *

Node.prototype.findSuccessorId = function *(id) {
	var preId = yield *this.findPredecessorId(id)
	return yield *this.pull(preId, 'successorId')
}

Node.prototype.findPredecessorId = function *(id) {
	var nodeId = this.id,
		successorId = this.successorId,
		hops = 0
	while (!NodeId.inRange(nodeId, id, successorId) && id !== successorId) &&
		   hops ++ < this.opts.queryTTL) {
		nodeId = yield *this.pull(nodeId, 'closestPrecedingId', id)
		successorId = yield *this.pull(nodeId, 'successorId')
	}
	if (hops > this.opts.queryTTL)
		debug('[%s] max hops achived when trying to find node %s', this.id, id)
	return nodeId
}

*/

Node.prototype.findSuccessorId = function *(id, ttl) {
	if (ttl === undefined) ttl = this.opts.queryTTL
	if (NodeId.inRange(this.id, id, this.successorId) || id === this.successorId) {
		return this.successorId
	}
	else if (ttl -- > 0) {
		var nextId = this._closestPrecedingFingerId(id)
		return nextId === this.id ? this.successorId :
			yield *this.pull(nextId, 'findSuccessorId', { id, ttl })
	}
	else {
		throw 'ttl'
	}
}

Node.prototype.findPredecessorId = function *(id, ttl) {
	if (ttl === undefined) ttl = this.opts.queryTTL
	if (NodeId.inRange(this.id, id, this.successorId) || id === this.successorId) {
		return this.id
	}
	else if (ttl -- > 0) {
		var nextId = this._closestPrecedingFingerId(id)
		return nextId === this.id ? this.id :
			yield *this.pull(nextId, 'findPredecessorId', { id, ttl })
	}
	else {
		throw 'ttl'
	}
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
	this.successorId = yield *this.pull(bootstrapId, 'findSuccessorId', { id:this.id })
	this.succBackupIds = yield *this.pull(this.successorId, 'succWithBackupIds')

	// Note: The following steps are not necessary from the original papers,
	//       but doing these before we start affecting the network makes it robuster

	// connect the predecessor now or will be connected later
	this.predecessorId = yield *this.pull(bootstrapId, 'findPredecessorId', { id:this.id })
	if (this.predecessorId) yield *this.pull(this.predecessorId, 'successorId')

	// initialize fingers except the first one (the successor)
	for (var i = 1; i < this.fingerIds.length; i ++) {
		var id = NodeId.shift(this.id, i)
		this.fingerIds[i] = yield *this.pull(bootstrapId, 'findSuccessorId', { id })
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

Node.prototype.pull = function *(id, method, arg) {
	if (id === this.id) {
		if (method === 'successorId')
			return this.successorId
		else if (method === 'predecessorId')
			return this.predecessorId
		else if (method === 'succWithBackupIds')
			return this._succWithBackupIds()
		else if (method === 'closestPrecedingId')
			return this._closestPrecedingFingerId(arg)
		else if (method === 'notify')
			return this._notify(arg)
		else if (method === 'findSuccessorId')
			return yield *this.findSuccessorId(arg.id, arg.ttl)
		else if (method === 'findPredecessorId')
			return yield *this.findPredecessorId(arg.id, arg.ttl)
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
